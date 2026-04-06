const { v4: uuidv4 } = require('uuid');
const NotificationSchedule = require('../models/NotificationSchedule');
const { createNotification } = require('./notificationService');
const logger = require('../utils/logger');

const DEFAULT_REMINDER_OFFSETS_HOURS = [48, 24, 12];

const scheduleTicketReminders = async ({
  recipientAuthId,
  recipientRole = 'USER',
  eventId,
  eventTitle,
  eventStartAt,
  actionUrl = null,
  metadata = {},
}) => {
  const authId = String(recipientAuthId || '').trim();
  const eid = String(eventId || '').trim();
  if (!authId || !eid || !eventStartAt) return [];

  const startAt = new Date(eventStartAt);
  if (Number.isNaN(startAt.getTime())) return [];

  const created = [];
  const now = Date.now();

  for (const offsetHours of DEFAULT_REMINDER_OFFSETS_HOURS) {
    const triggerAt = new Date(startAt.getTime() - (offsetHours * 60 * 60 * 1000));
    if (triggerAt.getTime() <= now) continue;

    const dedupeKey = `TICKET_EVENT_REMINDER:${eid}:${authId}:${offsetHours}`;

    const existing = await NotificationSchedule.findOne({
      recipientAuthId: authId,
      dedupeKey,
    }).lean();

    if (existing) {
      created.push(existing);
      continue;
    }

    try {
      const schedule = await NotificationSchedule.create({
        scheduleId: uuidv4(),
        recipientAuthId: authId,
        recipientRole: String(recipientRole || 'USER').trim().toUpperCase(),
        eventId: eid,
        type: 'TICKET_EVENT_REMINDER',
        category: 'SYSTEM',
        title: 'Event Reminder',
        message: `${String(eventTitle || 'Your event').trim()} starts in ${offsetHours} hour(s).`,
        actionUrl,
        metadata: {
          ...metadata,
          eventStartAt: startAt.toISOString(),
          offsetHours,
        },
        dedupeKey,
        triggerAt,
        status: 'PENDING',
      });

      created.push(schedule.toObject());
    } catch (error) {
      if (error?.code !== 11000) {
        logger.warn('Failed to schedule ticket reminder', {
          dedupeKey,
          message: error?.message || String(error),
        });
      }
    }
  }

  return created;
};

const processDueReminderSchedules = async ({ batchSize = 100 } = {}) => {
  const now = new Date();

  const due = await NotificationSchedule.find({
    status: 'PENDING',
    triggerAt: { $lte: now },
  })
    .sort({ triggerAt: 1 })
    .limit(Math.max(1, Number(batchSize) || 100));

  if (!due.length) return { processed: 0, sent: 0, failed: 0 };

  let sent = 0;
  let failed = 0;

  for (const row of due) {
    try {
      await createNotification({
        recipientAuthId: row.recipientAuthId,
        recipientRole: row.recipientRole,
        type: row.type,
        category: row.category,
        title: row.title,
        message: row.message,
        actionUrl: row.actionUrl,
        metadata: row.metadata || {},
        dedupeKey: `${row.dedupeKey}:DELIVERY`,
      });

      row.status = 'SENT';
      row.processedAt = new Date();
      row.lastError = null;
      row.attempts = Number(row.attempts || 0) + 1;
      await row.save();

      sent += 1;
    } catch (error) {
      row.status = 'FAILED';
      row.processedAt = new Date();
      row.lastError = String(error?.message || error).slice(0, 500);
      row.attempts = Number(row.attempts || 0) + 1;
      await row.save();

      failed += 1;
    }
  }

  return {
    processed: due.length,
    sent,
    failed,
  };
};

module.exports = {
  scheduleTicketReminders,
  processDueReminderSchedules,
};
