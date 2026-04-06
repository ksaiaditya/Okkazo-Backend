const Planning = require('../models/Planning');
const Promote = require('../models/Promote');
const logger = require('../utils/logger');
const { fetchActiveManagers } = require('../services/userServiceClient');
const { TERMINAL_STATUSES: PLANNING_TERMINAL_STATUSES } = require('../utils/planningConstants');
const { PROMOTE_STATUS } = require('../utils/promoteConstants');
const { ADMIN_DECISION_STATUS } = require('../utils/promoteConstants');
const { toIstDayString, parseIstDayStart } = require('../utils/istDateTime');

const normalizeLoose = (value) => String(value || '').trim().toLowerCase();

const isCoordinatorAssignedRole = (assignedRole) => {
  const role = normalizeLoose(assignedRole);
  return role.includes('coordinator');
};

const normalizeRangeToIstDaySpan = (range) => {
  const startDay = toIstDayString(range?.start);
  const endDay = toIstDayString(range?.end || range?.start);
  if (!startDay || !endDay) return null;

  const start = parseIstDayStart(startDay);
  const end = parseIstDayStart(endDay);
  if (!start || !end) return null;
  if (end < start) return { start, end: start };
  return { start, end };
};

const rangesOverlap = (a, b) => {
  const ra = normalizeRangeToIstDaySpan(a);
  const rb = normalizeRangeToIstDaySpan(b);

  if (!ra || !rb) return true;
  return ra.start <= rb.end && rb.start <= ra.end;
};

const planningToRange = (planning) => {
  const category = String(planning?.category || '').trim().toLowerCase();
  if (category === 'public') {
    return {
      start: planning?.schedule?.startAt,
      end: planning?.schedule?.endAt,
    };
  }

  return {
    start: planning?.eventDate,
    end: planning?.eventDate,
  };
};

const promoteToRange = (promote) => ({
  start: promote?.schedule?.startAt,
  end: promote?.schedule?.endAt,
});

const resolveTargetEventRange = async (eventId) => {
  const normalizedEventId = String(eventId || '').trim();
  if (!normalizedEventId) return null;

  const [planning, promote] = await Promise.all([
    Planning.findOne({ eventId: normalizedEventId })
      .select('eventId category schedule eventDate')
      .lean(),
    Promote.findOne({ eventId: normalizedEventId })
      .select('eventId schedule')
      .lean(),
  ]);

  if (planning) return planningToRange(planning);
  if (promote) return promoteToRange(promote);
  return null;
};

/**
 * GET /staff/core/available
 * Returns MANAGER users with assignedRole COORDINATOR who are available for the target event dates.
 * Optional query: ?excludeEventId=... (target event id used to compute overlap window and ignored as a conflict source)
 */
const getAvailableCoreStaff = async (req, res) => {
  try {
    const excludeEventId = String(req.query?.excludeEventId || '').trim();
    const targetRange = await resolveTargetEventRange(excludeEventId);

    const managers = await fetchActiveManagers({ limit: 500 });
    const coordinatorManagers = (managers || []).filter((u) => {
      if (!u) return false;
      if (normalizeLoose(u.role) !== 'manager') return false;
      if (u.isActive === false) return false;
      if (!isCoordinatorAssignedRole(u.assignedRole)) return false;
      return true;
    });

    const [planningAssignments, promoteAssignments] = await Promise.all([
      Planning.find({
        status: { $nin: PLANNING_TERMINAL_STATUSES },
        $or: [
          { assignedManagerId: { $ne: null } },
          { coreStaffIds: { $exists: true, $ne: [] } },
        ],
      })
        .select('eventId category schedule eventDate assignedManagerId coreStaffIds')
        .lean(),
      Promote.find({
        eventStatus: { $ne: PROMOTE_STATUS.COMPLETE },
        'adminDecision.status': { $ne: ADMIN_DECISION_STATUS.REJECTED },
        $or: [
          { assignedManagerId: { $ne: null } },
          { coreStaffIds: { $exists: true, $ne: [] } },
        ],
      })
        .select('eventId schedule assignedManagerId coreStaffIds')
        .lean(),
    ]);

    const busyManagerIds = new Set();

    for (const rec of planningAssignments || []) {
      if (excludeEventId && String(rec.eventId || '').trim() === excludeEventId) continue;
      if (targetRange && !rangesOverlap(targetRange, planningToRange(rec))) continue;

      if (rec?.assignedManagerId) {
        busyManagerIds.add(String(rec.assignedManagerId));
      }

      for (const staffId of rec?.coreStaffIds || []) {
        if (!staffId) continue;
        busyManagerIds.add(String(staffId));
      }
    }
    for (const rec of promoteAssignments || []) {
      if (excludeEventId && String(rec.eventId || '').trim() === excludeEventId) continue;
      if (targetRange && !rangesOverlap(targetRange, promoteToRange(rec))) continue;

      if (rec?.assignedManagerId) {
        busyManagerIds.add(String(rec.assignedManagerId));
      }

      for (const staffId of rec?.coreStaffIds || []) {
        if (!staffId) continue;
        busyManagerIds.add(String(staffId));
      }
    }

    const available = coordinatorManagers
      .filter((u) => {
        const id = String(u?._id || u?.id || '').trim();
        if (!id) return false;
        return !busyManagerIds.has(id);
      })
      .map((u) => ({
        id: String(u?._id || u?.id || '').trim(),
        authId: u?.authId || null,
        name: u?.name || null,
        email: u?.email || null,
        department: u?.department || null,
        assignedRole: u?.assignedRole || null,
        avatar: u?.avatar || null,
      }));

    return res.status(200).json({
      success: true,
      data: { staff: available },
    });
  } catch (error) {
    logger.error('Error in getAvailableCoreStaff:', error);
    return res.status(error.statusCode || 500).json({
      success: false,
      message: error.message || 'Failed to fetch available core staff',
    });
  }
};

module.exports = {
  getAvailableCoreStaff,
};
