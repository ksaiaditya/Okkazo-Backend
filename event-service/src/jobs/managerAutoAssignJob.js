const Promote = require('../models/Promote');
const Planning = require('../models/Planning');
const logger = require('../utils/logger');
const { fetchActiveManagers } = require('../services/userServiceClient');
const { CATEGORY, TERMINAL_STATUSES } = require('../utils/planningConstants');
const { PROMOTE_STATUS } = require('../utils/promoteConstants');
const { getState: getManagerAutoAssignState } = require('./managerAutoAssignRuntimeConfig');
const vendorSelectionService = require('../services/vendorSelectionService');
const promoteService = require('../services/promoteService');

const DEPARTMENT_PUBLIC = 'Public Event';
const DEPARTMENT_PRIVATE = 'Private Event';

const REQUIRED_DEPARTMENT_BY_PLANNING_CATEGORY = {
  [CATEGORY.PUBLIC]: DEPARTMENT_PUBLIC,
  [CATEGORY.PRIVATE]: DEPARTMENT_PRIVATE,
};

const normalizeLoose = (value) => String(value || '').trim().toLowerCase();

const promoteCandidateFilter = {
  assignedManagerId: null,
  eventStatus: { $ne: PROMOTE_STATUS.COMPLETE },
  'adminDecision.status': 'APPROVED',
  'adminDecision.decidedAt': { $ne: null },
};

const planningCandidateFilter = {
  assignedManagerId: null,
  status: { $nin: TERMINAL_STATUSES },
  $or: [{ vendorSelectionId: { $ne: null } }, { platformFeePaid: true }, { isPaid: true }],
};

let intervalHandle = null;
let startupTimeoutHandle = null;
let running = false;

const cursorByDepartment = new Map();

const isAssignedRoleEligible = (assignedRole) => {
  if (!assignedRole) return false;
  const role = normalizeLoose(assignedRole);
  return role.includes('junior') || role.includes('senior');
};

const getConfig = () => {
  const enabled = Boolean(getManagerAutoAssignState()?.enabled);
  const intervalMs = Math.max(10_000, Number(process.env.MANAGER_AUTOASSIGN_INTERVAL_MS || 60_000));
  const queryLimitPerType = Math.min(200, Math.max(1, Number(process.env.MANAGER_AUTOASSIGN_QUERY_LIMIT || 30)));
  const maxAssignmentsPerRun = Math.min(200, Math.max(1, Number(process.env.MANAGER_AUTOASSIGN_MAX_ASSIGNMENTS_PER_RUN || 40)));
  const managerFetchLimit = Math.min(2000, Math.max(1, Number(process.env.MANAGER_AUTOASSIGN_MANAGER_FETCH_LIMIT || 500)));
  const assignedByAuthId = String(process.env.MANAGER_AUTOASSIGN_AUTH_ID || 'system:autoassign').trim();

  return {
    enabled,
    intervalMs,
    queryLimitPerType,
    maxAssignmentsPerRun,
    managerFetchLimit,
    assignedByAuthId,
  };
};

const toDate = (value) => {
  const d = value ? new Date(value) : null;
  return d && !Number.isNaN(d.getTime()) ? d : null;
};

const getEventStartDate = ({ requestType, doc }) => {
  if (requestType === 'PROMOTE') return toDate(doc?.schedule?.startAt);
  // Planning private category uses eventDate; public uses schedule.startAt
  if (String(doc?.category || '').toUpperCase() === CATEGORY.PRIVATE) return toDate(doc?.eventDate);
  return toDate(doc?.schedule?.startAt) || toDate(doc?.eventDate);
};

const startOfDay = (date) => new Date(date.getFullYear(), date.getMonth(), date.getDate(), 0, 0, 0, 0);
const endOfDayExclusive = (date) => new Date(date.getFullYear(), date.getMonth(), date.getDate() + 1, 0, 0, 0, 0);

const buildScheduleOverlapOr = ({ start, end }) => {
  // schedule overlaps day window
  return [
    { 'schedule.startAt': { $gte: start, $lt: end } },
    { 'schedule.endAt': { $gt: start, $lte: end } },
    { 'schedule.startAt': { $lt: start }, 'schedule.endAt': { $gt: end } },
  ];
};

const isManagerAvailableForDay = async ({ managerId, dayStart, dayEnd, excludeEventId } = {}) => {
  if (!managerId) return false;

  const promoteQuery = {
    assignedManagerId: managerId,
    eventStatus: { $ne: PROMOTE_STATUS.COMPLETE },
    'adminDecision.status': { $ne: 'REJECTED' },
    ...(excludeEventId ? { eventId: { $ne: String(excludeEventId).trim() } } : {}),
    $or: buildScheduleOverlapOr({ start: dayStart, end: dayEnd }),
  };

  const planningQuery = {
    assignedManagerId: managerId,
    status: { $nin: TERMINAL_STATUSES },
    ...(excludeEventId ? { eventId: { $ne: String(excludeEventId).trim() } } : {}),
    $or: [
      { eventDate: { $gte: dayStart, $lt: dayEnd } },
      ...buildScheduleOverlapOr({ start: dayStart, end: dayEnd }),
    ],
  };

  const [existingPromote, existingPlanning] = await Promise.all([
    Promote.findOne(promoteQuery).select('eventId').lean(),
    Planning.findOne(planningQuery).select('eventId').lean(),
  ]);

  return !existingPromote && !existingPlanning;
};

const loadEligibleManagersByDepartment = async ({ limit } = {}) => {
  const managers = await fetchActiveManagers({ limit });
  const buckets = new Map();

  for (const manager of managers || []) {
    if (normalizeLoose(manager?.role) !== 'manager') continue;
    if (manager?.isActive === false) continue;
    if (!isAssignedRoleEligible(manager?.assignedRole)) continue;

    const id = String(manager?._id || manager?.id || '').trim();
    if (!id) continue;
    const deptKey = normalizeLoose(manager?.department);
    if (!deptKey) continue;

    if (!buckets.has(deptKey)) buckets.set(deptKey, []);
    buckets.get(deptKey).push(id);
  }

  // Stable ordering for deterministic RR.
  for (const [key, ids] of buckets.entries()) {
    buckets.set(key, Array.from(new Set(ids)).sort());
  }

  return buckets;
};

const pickRoundRobin = ({ deptKey, managerIds } = {}) => {
  if (!deptKey || !Array.isArray(managerIds) || managerIds.length === 0) return null;
  const key = normalizeLoose(deptKey);
  const cursor = Math.max(0, Number(cursorByDepartment.get(key) || 0));
  const idx = cursor % managerIds.length;
  cursorByDepartment.set(key, (idx + 1) % managerIds.length);
  return String(managerIds[idx]);
};

const pickAvailableManagerForEvent = async ({ deptKey, managerIds, dayStart, dayEnd, excludeEventId } = {}) => {
  if (!Array.isArray(managerIds) || managerIds.length === 0) return null;

  // Try all managers once, starting from RR cursor.
  const size = managerIds.length;
  const key = normalizeLoose(deptKey);
  const startCursor = Math.max(0, Number(cursorByDepartment.get(key) || 0));

  for (let offset = 0; offset < size; offset += 1) {
    const idx = (startCursor + offset) % size;
    const candidateId = String(managerIds[idx]);

    const ok = await isManagerAvailableForDay({
      managerId: candidateId,
      dayStart,
      dayEnd,
      excludeEventId,
    });

    if (ok) {
      cursorByDepartment.set(key, (idx + 1) % size);
      return candidateId;
    }
  }

  // No available manager for that day.
  return null;
};

const runOnce = async () => {
  if (running) return;
  running = true;

  const config = getConfig();

  try {
    if (!config.enabled) return;

    // Fast pre-check: avoid user-service call if nothing to do.
    const [hasPromote, hasPlanning] = await Promise.all([
      Promote.exists(promoteCandidateFilter),
      Planning.exists(planningCandidateFilter),
    ]);
    if (!hasPromote && !hasPlanning) return;

    const managerBuckets = await loadEligibleManagersByDepartment({ limit: config.managerFetchLimit });

    let assignedTotal = 0;

    // 1) Promote (Public Event)
    if (hasPromote && assignedTotal < config.maxAssignmentsPerRun) {
      const candidates = await Promote.find(promoteCandidateFilter)
        .sort({ 'adminDecision.decidedAt': 1 })
        .limit(config.queryLimitPerType)
        .select('eventId schedule.startAt schedule.endAt')
        .lean();

      const deptKey = normalizeLoose(DEPARTMENT_PUBLIC);
      const managerIds = managerBuckets.get(deptKey) || [];

      for (const candidate of candidates || []) {
        if (assignedTotal >= config.maxAssignmentsPerRun) break;

        const eventStart = getEventStartDate({ requestType: 'PROMOTE', doc: candidate });
        if (!eventStart) continue;
        const dayStart = startOfDay(eventStart);
        const dayEnd = endOfDayExclusive(eventStart);

        const managerId = await pickAvailableManagerForEvent({
          deptKey,
          managerIds,
          dayStart,
          dayEnd,
          excludeEventId: candidate.eventId,
        });
        if (!managerId) break;

        try {
          const result = await promoteService.tryAutoAssignManager(candidate.eventId, managerId, {
            assignedByAuthId: config.assignedByAuthId || null,
          });

          if (result?.assigned) {
            assignedTotal += 1;
            logger.info(`Auto-assigned manager ${managerId} to promote ${candidate.eventId}`);
          }
        } catch (error) {
          logger.warn(`Auto-assign (promote) failed for ${candidate.eventId}: ${error.message}`);
        }
      }
    }

    // 2) Planning (Public vs Private)
    if (hasPlanning && assignedTotal < config.maxAssignmentsPerRun) {
      const candidates = await Planning.find(planningCandidateFilter)
        .sort({ createdAt: 1 })
        .limit(config.queryLimitPerType)
        .select('eventId category eventDate schedule.startAt schedule.endAt')
        .lean();

      for (const candidate of candidates || []) {
        if (assignedTotal >= config.maxAssignmentsPerRun) break;

        const requiredDepartment = REQUIRED_DEPARTMENT_BY_PLANNING_CATEGORY[candidate?.category] || null;
        if (!requiredDepartment) continue;

        const deptKey = normalizeLoose(requiredDepartment);
        const managerIds = managerBuckets.get(deptKey) || [];

        const eventStart = getEventStartDate({ requestType: 'PLANNING', doc: candidate });
        if (!eventStart) continue;
        const dayStart = startOfDay(eventStart);
        const dayEnd = endOfDayExclusive(eventStart);

        const managerId = await pickAvailableManagerForEvent({
          deptKey,
          managerIds,
          dayStart,
          dayEnd,
          excludeEventId: candidate.eventId,
        });

        if (!managerId) continue;

        try {
          const updateResult = await Planning.updateOne(
            {
              eventId: String(candidate.eventId).trim(),
              assignedManagerId: null,
              status: { $nin: TERMINAL_STATUSES },
              $or: [{ vendorSelectionId: { $ne: null } }, { platformFeePaid: true }, { isPaid: true }],
            },
            {
              $set: {
                assignedManagerId: managerId,
              },
            }
          );

          if (updateResult?.modifiedCount === 1) {
            assignedTotal += 1;
            logger.info(`Auto-assigned manager ${managerId} to planning ${candidate.eventId}`);

            // Best-effort: keep VendorSelection manager fields aligned.
            try {
              const planning = await Planning.findOne({ eventId: String(candidate.eventId).trim() });
              if (planning) {
                await vendorSelectionService.ensureForPlanning(planning);
              }
            } catch (error) {
              logger.warn(`VendorSelection sync after auto-assign (planning) failed for ${candidate.eventId}: ${error.message}`);
            }
          }
        } catch (error) {
          logger.warn(`Auto-assign (planning) failed for ${candidate.eventId}: ${error.message}`);
        }
      }
    }
  } catch (error) {
    logger.error('Manager auto-assign job failed:', error);
  } finally {
    running = false;
  }
};

const startManagerAutoAssignJob = () => {
  const config = getConfig();
  if (!config.enabled) {
    logger.info('Manager auto-assign job disabled');
    return;
  }

  if (intervalHandle) return;

  logger.info(
    `Manager auto-assign job started (interval=${config.intervalMs}ms, strategy=round_robin, maxPerRun=${config.maxAssignmentsPerRun})`
  );

  intervalHandle = setInterval(() => {
    runOnce().catch((err) => logger.error('Manager auto-assign tick failed:', err));
  }, config.intervalMs);

  // Run soon after startup.
  startupTimeoutHandle = setTimeout(() => {
    runOnce().catch(() => null);
  }, 5_000);
};

const stopManagerAutoAssignJob = () => {
  if (!intervalHandle && !startupTimeoutHandle) return;
  if (startupTimeoutHandle) {
    clearTimeout(startupTimeoutHandle);
    startupTimeoutHandle = null;
  }
  if (!intervalHandle) return;
  clearInterval(intervalHandle);
  intervalHandle = null;
  logger.info('Manager auto-assign job stopped');
};

module.exports = {
  startManagerAutoAssignJob,
  stopManagerAutoAssignJob,
  runOnce,
};
