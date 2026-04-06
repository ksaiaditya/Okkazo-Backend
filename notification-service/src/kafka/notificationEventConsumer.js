const kafkaConfig = require('../config/kafka');
const notificationService = require('../services/notificationService');
const {
  resolveAuthIdFromIdentifier,
  fetchUsersByRole,
} = require('../services/userDirectoryService');
const { scheduleTicketReminders } = require('../services/reminderService');
const logger = require('../utils/logger');

let consumer = null;

const parseDate = (value) => {
  if (!value) return null;
  const dt = new Date(value);
  return Number.isNaN(dt.getTime()) ? null : dt;
};

const toUpper = (value) => String(value || '').trim().toUpperCase();

const eventTopics = (process.env.KAFKA_TOPICS || 'auth_events,event_events,payment_events,admin_events,vendor_events')
  .split(',')
  .map((topic) => topic.trim())
  .filter(Boolean);

const createNotificationSafe = async (payload) => {
  try {
    await notificationService.createNotification(payload);
  } catch (error) {
    logger.warn('Failed to create notification', {
      recipientAuthId: payload?.recipientAuthId,
      type: payload?.type,
      message: error?.message || String(error),
    });
  }
};

const notifyAdmins = async ({ type, category, title, message, actionUrl = null, metadata = {}, dedupePrefix = '' }) => {
  const admins = await fetchUsersByRole('ADMIN');
  if (!Array.isArray(admins) || admins.length === 0) return;

  const notifications = admins
    .map((admin) => ({
      recipientAuthId: String(admin?.authId || '').trim(),
      recipientRole: 'ADMIN',
      type,
      category,
      title,
      message,
      actionUrl,
      metadata,
      dedupeKey: `${dedupePrefix}:${String(admin?.authId || '').trim()}`,
    }))
    .filter((row) => row.recipientAuthId);

  await notificationService.createNotificationsBulk(notifications);
};

const LIFECYCLE_STATUS_LABEL = {
  CONFIRMED: 'confirmed',
  LIVE: 'live',
  COMPLETE: 'completed',
  COMPLETED: 'completed',
  CLOSED: 'closed',
};

const VENDOR_PROGRESS_DEDUPE_WINDOW_MINUTES = Math.max(
  1,
  Number(process.env.VENDOR_PROGRESS_DEDUPE_WINDOW_MINUTES || 15)
);
const VENDOR_PROGRESS_WINDOW_MS = VENDOR_PROGRESS_DEDUPE_WINDOW_MINUTES * 60 * 1000;

const toCount = (value) => {
  const n = Number(value || 0);
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : 0;
};

const normalizeVendorProgress = (progress = {}) => {
  const accepted = toCount(progress?.accepted);
  const rejected = toCount(progress?.rejected);
  const pending = toCount(progress?.pending);
  const total = toCount(progress?.total) || (accepted + rejected + pending);

  if (total < 1) return null;
  return {
    accepted,
    rejected,
    pending,
    total,
  };
};

const toDedupeWindowBucket = (occurredAt = null) => {
  const parsed = parseDate(occurredAt);
  const timestamp = parsed ? parsed.getTime() : Date.now();
  return Math.floor(timestamp / VENDOR_PROGRESS_WINDOW_MS);
};

const maybeNotifyVendorProgressAggregate = async ({
  eventId,
  eventTitle,
  authId,
  managerAuthId,
  progress,
  occurredAt,
}) => {
  const normalizedProgress = normalizeVendorProgress(progress);
  if (!normalizedProgress) return;

  const userAuthId = String(authId || '').trim();
  const managerResolvedAuthId = await resolveAuthIdFromIdentifier(managerAuthId);
  const bucket = toDedupeWindowBucket(occurredAt);

  const recipients = [
    { authId: userAuthId, role: 'USER', actionUrl: '/user/my-events' },
    { authId: managerResolvedAuthId, role: 'MANAGER', actionUrl: '/manager/events' },
  ].filter((row) => String(row?.authId || '').trim());

  if (recipients.length === 0) return;

  const message = `${normalizedProgress.accepted} of ${normalizedProgress.total} vendor requests accepted${
    normalizedProgress.pending > 0 ? `, ${normalizedProgress.pending} pending.` : '.'
  }`;

  for (const recipient of recipients) {
    await createNotificationSafe({
      recipientAuthId: recipient.authId,
      recipientRole: recipient.role,
      type: 'VENDOR_ACCEPTANCE_PROGRESS',
      category: 'EVENT',
      title: 'Vendor acceptance progress',
      message,
      actionUrl: recipient.actionUrl,
      metadata: {
        eventId: eventId || null,
        eventTitle: eventTitle || null,
        ...normalizedProgress,
      },
      dedupeKey: `VENDOR_PROGRESS:${eventId || 'na'}:${recipient.authId}:${bucket}`,
    });
  }
};

const notifyLifecycleStakeholders = async ({
  eventId,
  status,
  eventType,
  assignedManagerId,
  vendorAuthIds,
  eventTitle,
}) => {
  const statusUpper = toUpper(status);
  if (!statusUpper) return;

  const statusLabel = LIFECYCLE_STATUS_LABEL[statusUpper];
  if (!statusLabel) return;

  const normalizedEventType = String(eventType || 'event').trim().toLowerCase();
  const titleSuffix = eventTitle ? `: ${eventTitle}` : '';

  const managerAuthId = await resolveAuthIdFromIdentifier(assignedManagerId);
  if (managerAuthId) {
    await createNotificationSafe({
      recipientAuthId: managerAuthId,
      recipientRole: 'MANAGER',
      type: `EVENT_${statusUpper}`,
      category: 'EVENT',
      title: `Event ${statusLabel}`,
      message: `A ${normalizedEventType} event${titleSuffix} is now ${statusLabel}.`,
      actionUrl: '/manager/events',
      metadata: {
        eventId: eventId || null,
        status: statusUpper,
        eventType: normalizedEventType,
      },
      dedupeKey: `MANAGER_EVENT_STATUS:${normalizedEventType}:${eventId || 'na'}:${statusUpper}:${managerAuthId}`,
    });
  }

  await notifyAdmins({
    type: `EVENT_${statusUpper}`,
    category: 'EVENT',
    title: `Event ${statusLabel}`,
    message: `${normalizedEventType} event${titleSuffix} moved to ${statusLabel}.`,
    actionUrl: '/admin/events',
    metadata: {
      eventId: eventId || null,
      status: statusUpper,
      eventType: normalizedEventType,
    },
    dedupePrefix: `ADMIN_EVENT_STATUS:${normalizedEventType}:${eventId || 'na'}:${statusUpper}`,
  });

  if (statusUpper === 'CONFIRMED' || statusUpper === 'CLOSED') {
    const uniqueVendorAuthIds = Array.from(
      new Set((Array.isArray(vendorAuthIds) ? vendorAuthIds : [])
        .map((id) => String(id || '').trim())
        .filter(Boolean))
    );

    if (uniqueVendorAuthIds.length > 0) {
      const vendorNotifications = uniqueVendorAuthIds.map((vendorAuthId) => ({
        recipientAuthId: vendorAuthId,
        recipientRole: 'VENDOR',
        type: `EVENT_${statusUpper}`,
        category: 'EVENT',
        title: `Event ${statusLabel}`,
        message: `A booking${titleSuffix} is now ${statusLabel}.`,
        actionUrl: '/vendor/booked-events',
        metadata: {
          eventId: eventId || null,
          status: statusUpper,
          eventType: normalizedEventType,
        },
        dedupeKey: `VENDOR_EVENT_STATUS:${normalizedEventType}:${eventId || 'na'}:${statusUpper}:${vendorAuthId}`,
      }));

      await notificationService.createNotificationsBulk(vendorNotifications);
    }
  }
};

const handleAuthEvent = async (payload) => {
  const eventType = toUpper(payload?.type || payload?.eventType);
  const authId = String(payload?.authId || '').trim();

  switch (eventType) {
    case 'USER_REGISTERED':
    case 'USER_GOOGLE_REGISTERED': {
      if (!authId) return;
      await createNotificationSafe({
        recipientAuthId: authId,
        recipientRole: 'USER',
        type: 'WELCOME',
        category: 'SYSTEM',
        title: 'Welcome to Okkazo',
        message: 'Your account is ready. Start planning and managing your events.',
        actionUrl: '/user/dashboard',
        metadata: { sourceEventType: eventType },
        dedupeKey: `WELCOME:${authId}`,
      });
      break;
    }

    case 'MANAGER_ACCOUNT_CREATED': {
      if (!authId) return;
      await createNotificationSafe({
        recipientAuthId: authId,
        recipientRole: 'MANAGER',
        type: 'WELCOME',
        category: 'SYSTEM',
        title: 'Manager account created',
        message: 'Your manager account is active. Review assigned events from your dashboard.',
        actionUrl: '/manager/dashboard',
        metadata: { sourceEventType: eventType },
        dedupeKey: `WELCOME_MANAGER:${authId}`,
      });
      break;
    }

    case 'VENDOR_ACCOUNT_CREATED': {
      if (!authId) return;
      await createNotificationSafe({
        recipientAuthId: authId,
        recipientRole: 'VENDOR',
        type: 'WELCOME',
        category: 'SYSTEM',
        title: 'Vendor account created',
        message: 'Your vendor account has been created. Complete profile and start accepting bookings.',
        actionUrl: '/vendor/dashboard',
        metadata: { sourceEventType: eventType },
        dedupeKey: `WELCOME_VENDOR:${authId}`,
      });
      break;
    }

    default:
      break;
  }
};

const notifyUserEventStatus = async ({ authId, status, eventId, eventType = 'planning', assignedManagerId = null }) => {
  const safeAuthId = String(authId || '').trim();
  if (!safeAuthId) return;

  const statusUpper = toUpper(status);
  const labelMap = {
    PENDING_APPROVAL: 'pending approval',
    APPROVED: 'approved',
    CONFIRMED: 'confirmed',
    LIVE: 'live',
    COMPLETE: 'completed',
    COMPLETED: 'completed',
    CLOSED: 'closed',
    REJECTED: 'rejected',
  };

  const friendly = labelMap[statusUpper] || String(status || 'updated').toLowerCase();
  const basePath = eventType === 'promote' ? '/user/my-events' : '/user/my-events';

  await createNotificationSafe({
    recipientAuthId: safeAuthId,
    recipientRole: 'USER',
    type: 'EVENT_STATUS_UPDATED',
    category: 'EVENT',
    title: 'Event status updated',
    message: `Your ${eventType} event ${eventId ? `(${eventId}) ` : ''}is now ${friendly}.`,
    actionUrl: basePath,
    metadata: {
      eventId,
      status: statusUpper,
      eventType,
    },
    dedupeKey: `EVENT_STATUS:${eventType}:${eventId || 'na'}:${statusUpper}:${safeAuthId}`,
  });

  if (assignedManagerId) {
    const managerAuthId = await resolveAuthIdFromIdentifier(assignedManagerId);
    if (managerAuthId) {
      await createNotificationSafe({
        recipientAuthId: managerAuthId,
        recipientRole: 'MANAGER',
        type: 'EVENT_ASSIGNED',
        category: 'EVENT',
        title: 'New event assigned',
        message: `A ${eventType} event ${eventId ? `(${eventId}) ` : ''}has been assigned to you.`,
        actionUrl: '/manager/events',
        metadata: {
          eventId,
          eventType,
          assignedManagerId,
        },
        dedupeKey: `EVENT_ASSIGNED:${eventType}:${eventId || 'na'}:${managerAuthId}`,
      });
    }
  }
};

const handleEventEvent = async (payload) => {
  const eventType = toUpper(payload?.type || payload?.eventType);

  if (eventType === 'PLANNING_STATUS_UPDATED') {
    await notifyUserEventStatus({
      authId: payload?.authId,
      status: payload?.status,
      eventId: payload?.eventId,
      eventType: 'planning',
      assignedManagerId: payload?.assignedManagerId,
    });
    return;
  }

  if (eventType === 'PROMOTE_STATUS_UPDATED') {
    await notifyUserEventStatus({
      authId: payload?.authId,
      status: payload?.eventStatus,
      eventId: payload?.eventId,
      eventType: 'promote',
      assignedManagerId: payload?.assignedManagerId,
    });
    return;
  }

  if (eventType === 'PLANNING_VENDOR_CONFIRMATION_PAYMENT_CONFIRMED') {
    const authId = String(payload?.authId || '').trim();
    if (!authId) return;

    await createNotificationSafe({
      recipientAuthId: authId,
      recipientRole: 'USER',
      type: 'VENDOR_CONFIRMATION_PAYMENT_RECEIVED',
      category: 'PAYMENT',
      title: 'Vendor confirmation fee paid',
      message: 'Vendor confirmation fee is received. Your event is moving to confirmed state.',
      actionUrl: '/user/my-events',
      metadata: {
        eventId: payload?.eventId,
        status: payload?.status,
      },
      dedupeKey: `VENDOR_CONFIRMATION_FEE:${payload?.eventId || 'na'}:${authId}`,
    });
    return;
  }

  if (eventType === 'TICKET_PURCHASE_CONFIRMED') {
    const authId = String(payload?.authId || '').trim();
    if (!authId) return;

    const quantity = Number(payload?.quantity || 0);
    const eventTitle = String(payload?.eventTitle || 'your event').trim() || 'your event';

    await createNotificationSafe({
      recipientAuthId: authId,
      recipientRole: 'USER',
      type: 'TICKET_BOOKED',
      category: 'TICKET',
      title: 'Ticket booked successfully',
      message: `Your ticket${quantity > 1 ? 's' : ''} for ${eventTitle} ${quantity > 1 ? `(${quantity}) ` : ''}has been booked successfully.`,
      actionUrl: '/user/ticket-management',
      metadata: {
        eventId: payload?.eventId,
        ticketId: payload?.ticketId,
        quantity,
        tiers: payload?.tiers || [],
      },
      dedupeKey: `TICKET_BOOKED:${payload?.eventId || 'na'}:${payload?.ticketId || 'na'}:${authId}`,
    });

    const startAt = payload?.eventStartAt || payload?.schedule?.startAt || null;
    if (startAt) {
      await scheduleTicketReminders({
        recipientAuthId: authId,
        recipientRole: 'USER',
        eventId: payload?.eventId,
        eventTitle,
        eventStartAt: startAt,
        actionUrl: '/user/ticket-management',
        metadata: {
          ticketId: payload?.ticketId || null,
          sourceEventType: eventType,
        },
      });
    }

    return;
  }

  if (eventType === 'EVENT_LIFECYCLE_STATUS_UPDATED') {
    await notifyLifecycleStakeholders({
      eventId: payload?.eventId,
      status: payload?.status,
      eventType: payload?.eventType,
      assignedManagerId: payload?.assignedManagerId,
      vendorAuthIds: payload?.vendorAuthIds,
      eventTitle: payload?.eventTitle,
    });
    return;
  }

  if (eventType === 'VENDOR_BOOKING_REQUEST_RECEIVED') {
    const vendorAuthId = String(payload?.vendorAuthId || '').trim();
    if (!vendorAuthId) return;

    const eventTitle = String(payload?.eventTitle || 'an event').trim() || 'an event';

    await createNotificationSafe({
      recipientAuthId: vendorAuthId,
      recipientRole: 'VENDOR',
      type: 'BOOKING_REQUEST_RECEIVED',
      category: 'EVENT',
      title: 'New booking request',
      message: `You received a booking request for ${eventTitle}${payload?.service ? ` (${payload.service})` : ''}.`,
      actionUrl: '/vendor/booked-events',
      metadata: {
        eventId: payload?.eventId,
        eventTitle,
        service: payload?.service || null,
        progress: payload?.progress || null,
      },
      dedupeKey: `BOOKING_REQUEST_RECEIVED:${payload?.eventId || 'na'}:${payload?.service || 'na'}:${vendorAuthId}`,
    });

    return;
  }

  if (eventType === 'VENDOR_REQUEST_ACCEPTED') {
    const userAuthId = String(payload?.authId || '').trim();
    const vendorAuthId = String(payload?.vendorAuthId || '').trim();
    const managerAuthId = await resolveAuthIdFromIdentifier(payload?.managerAuthId);
    const eventTitle = String(payload?.eventTitle || 'your event').trim() || 'your event';
    const service = String(payload?.service || 'service').trim() || 'service';

    if (userAuthId) {
      await createNotificationSafe({
        recipientAuthId: userAuthId,
        recipientRole: 'USER',
        type: 'VENDOR_REQUEST_ACCEPTED',
        category: 'EVENT',
        title: 'Vendor request accepted',
        message: `${service} vendor accepted your booking request for ${eventTitle}.`,
        actionUrl: '/user/my-events',
        metadata: {
          eventId: payload?.eventId,
          service,
          vendorAuthId,
          eventTitle,
          progress: payload?.progress || null,
        },
        dedupeKey: `VENDOR_ACCEPTED_USER:${payload?.eventId || 'na'}:${service}:${userAuthId}`,
      });
    }

    if (managerAuthId) {
      await createNotificationSafe({
        recipientAuthId: managerAuthId,
        recipientRole: 'MANAGER',
        type: 'VENDOR_REQUEST_ACCEPTED',
        category: 'EVENT',
        title: 'Vendor accepted booking',
        message: `${service} vendor accepted booking for ${eventTitle}.`,
        actionUrl: '/manager/events',
        metadata: {
          eventId: payload?.eventId,
          service,
          vendorAuthId,
          eventTitle,
          progress: payload?.progress || null,
        },
        dedupeKey: `VENDOR_ACCEPTED_MANAGER:${payload?.eventId || 'na'}:${service}:${managerAuthId}`,
      });
    }

    if (vendorAuthId) {
      await createNotificationSafe({
        recipientAuthId: vendorAuthId,
        recipientRole: 'VENDOR',
        type: 'BOOKING_ACCEPTED',
        category: 'EVENT',
        title: 'Booking accepted',
        message: `You accepted booking for ${eventTitle}${service ? ` (${service})` : ''}.`,
        actionUrl: '/vendor/booked-events',
        metadata: {
          eventId: payload?.eventId,
          service,
          eventTitle,
        },
        dedupeKey: `BOOKING_ACCEPTED_VENDOR:${payload?.eventId || 'na'}:${service}:${vendorAuthId}`,
      });
    }

    await maybeNotifyVendorProgressAggregate({
      eventId: payload?.eventId,
      eventTitle,
      authId: userAuthId,
      managerAuthId: payload?.managerAuthId,
      progress: payload?.progress,
      occurredAt: payload?.occurredAt,
    });

    return;
  }

  if (eventType === 'VENDOR_REQUEST_REJECTED') {
    const userAuthId = String(payload?.authId || '').trim();
    const vendorAuthId = String(payload?.vendorAuthId || '').trim();
    const managerAuthId = await resolveAuthIdFromIdentifier(payload?.managerAuthId);
    const eventTitle = String(payload?.eventTitle || 'your event').trim() || 'your event';
    const service = String(payload?.service || 'service').trim() || 'service';

    if (userAuthId) {
      await createNotificationSafe({
        recipientAuthId: userAuthId,
        recipientRole: 'USER',
        type: 'VENDOR_REQUEST_REJECTED',
        category: 'EVENT',
        title: 'Vendor request rejected',
        message: `${service} vendor rejected your booking request for ${eventTitle}.`,
        actionUrl: '/user/my-events',
        metadata: {
          eventId: payload?.eventId,
          service,
          vendorAuthId,
          eventTitle,
          progress: payload?.progress || null,
        },
        dedupeKey: `VENDOR_REJECTED_USER:${payload?.eventId || 'na'}:${service}:${userAuthId}`,
      });
    }

    if (managerAuthId) {
      await createNotificationSafe({
        recipientAuthId: managerAuthId,
        recipientRole: 'MANAGER',
        type: 'VENDOR_REQUEST_REJECTED',
        category: 'EVENT',
        title: 'Vendor rejected booking',
        message: `${service} vendor rejected booking for ${eventTitle}.`,
        actionUrl: '/manager/events',
        metadata: {
          eventId: payload?.eventId,
          service,
          vendorAuthId,
          eventTitle,
          progress: payload?.progress || null,
        },
        dedupeKey: `VENDOR_REJECTED_MANAGER:${payload?.eventId || 'na'}:${service}:${managerAuthId}`,
      });
    }

    if (vendorAuthId) {
      await createNotificationSafe({
        recipientAuthId: vendorAuthId,
        recipientRole: 'VENDOR',
        type: 'BOOKING_REJECTED',
        category: 'EVENT',
        title: 'Booking rejected',
        message: `You rejected booking for ${eventTitle}${service ? ` (${service})` : ''}.`,
        actionUrl: '/vendor/booked-events',
        metadata: {
          eventId: payload?.eventId,
          service,
          eventTitle,
        },
        dedupeKey: `BOOKING_REJECTED_VENDOR:${payload?.eventId || 'na'}:${service}:${vendorAuthId}`,
      });
    }

    await maybeNotifyVendorProgressAggregate({
      eventId: payload?.eventId,
      eventTitle,
      authId: userAuthId,
      managerAuthId: payload?.managerAuthId,
      progress: payload?.progress,
      occurredAt: payload?.occurredAt,
    });

    return;
  }

  if (eventType === 'TICKET_VERIFIED') {
    const authId = String(payload?.authId || payload?.userAuthId || '').trim();
    if (!authId) return;

    const eventTitle = String(payload?.eventTitle || 'your event').trim() || 'your event';

    await createNotificationSafe({
      recipientAuthId: authId,
      recipientRole: 'USER',
      type: 'TICKET_VERIFIED',
      category: 'TICKET',
      title: 'Ticket verified',
      message: `Your ticket for ${eventTitle} has been verified at entry.`,
      actionUrl: '/user/ticket-management',
      metadata: {
        eventId: payload?.eventId,
        ticketId: payload?.ticketId,
        verifiedAt: payload?.verifiedAt || payload?.occurredAt || null,
      },
      dedupeKey: `TICKET_VERIFIED:${payload?.ticketId || 'na'}:${authId}`,
    });
    return;
  }

  if (eventType === 'VENDOR_REQUEST_REJECTED_ALTERNATIVES') {
    const authId = String(payload?.authId || '').trim();
    if (!authId) return;

    await createNotificationSafe({
      recipientAuthId: authId,
      recipientRole: 'USER',
      type: 'VENDOR_REQUEST_REJECTED',
      category: 'EVENT',
      title: 'Vendor request rejected',
      message: `Your ${payload?.service || 'service'} vendor request was rejected. Alternatives are available.`,
      actionUrl: '/user/my-events',
      metadata: {
        eventId: payload?.eventId,
        service: payload?.service,
        alternatives: payload?.options || [],
      },
      dedupeKey: `VENDOR_REJECTED:${payload?.eventId || 'na'}:${payload?.service || 'na'}:${authId}`,
    });
    return;
  }

  if (eventType === 'PROMOTION_EMAIL_BLAST_REQUESTED') {
    await notifyAdmins({
      type: 'PROMOTION_EMAIL_BLAST_REQUESTED',
      category: 'PROMOTION',
      title: 'Promotion email blast requested',
      message: `An email blast was requested for event ${payload?.eventId || ''}.`,
      actionUrl: '/admin/promotions',
      metadata: payload,
      dedupePrefix: `PROMOTION_EMAIL_BLAST:${payload?.eventId || 'na'}`,
    });
    return;
  }
};

const handlePaymentEvent = async (payload) => {
  const eventType = toUpper(payload?.type || payload?.eventType);

  if (eventType === 'PAYMENT_SUCCESS') {
    const authId = String(payload?.authId || '').trim();
    if (!authId) return;

    const orderType = String(payload?.orderType || 'payment').trim();
    if (toUpper(orderType) === 'TICKET SALE') return;

    await createNotificationSafe({
      recipientAuthId: authId,
      recipientRole: 'USER',
      type: 'PAYMENT_RECEIVED',
      category: 'PAYMENT',
      title: 'Payment received',
      message: `Payment for ${orderType} was received successfully.`,
      actionUrl: '/user/my-events',
      metadata: {
        eventId: payload?.eventId,
        orderType,
        amount: payload?.amount,
        currency: payload?.currency,
      },
      dedupeKey: `PAYMENT_SUCCESS:${payload?.paymentOrderId || payload?.transactionId || payload?.eventId || 'na'}:${authId}`,
    });
    return;
  }

  if (eventType === 'PAYMENT_REFUND_SUCCESS') {
    const authId = String(payload?.authId || '').trim();
    if (!authId) return;

    await createNotificationSafe({
      recipientAuthId: authId,
      recipientRole: 'USER',
      type: 'PAYMENT_REFUNDED',
      category: 'PAYMENT',
      title: 'Refund processed',
      message: 'Your payment refund was processed successfully.',
      actionUrl: '/user/my-events',
      metadata: payload,
      dedupeKey: `PAYMENT_REFUND:${payload?.paymentOrderId || payload?.transactionId || payload?.eventId || 'na'}:${authId}`,
    });
    return;
  }

  if (eventType === 'VENDOR_PAYOUT_SUCCESS') {
    const vendorAuthId = String(payload?.vendorAuthId || '').trim();
    const managerAuthId = String(payload?.managerAuthId || '').trim();

    if (vendorAuthId) {
      await createNotificationSafe({
        recipientAuthId: vendorAuthId,
        recipientRole: 'VENDOR',
        type: 'VENDOR_PAYOUT_RECEIVED',
        category: 'PAYOUT',
        title: 'Payout received',
        message: `Your payout for ${payload?.service || 'service'} has been released.`,
        actionUrl: '/vendor/ledger',
        metadata: payload,
        dedupeKey: `VENDOR_PAYOUT:${payload?.payoutId || payload?.eventId || 'na'}:${vendorAuthId}`,
      });
    }

    if (managerAuthId) {
      await createNotificationSafe({
        recipientAuthId: managerAuthId,
        recipientRole: 'MANAGER',
        type: 'VENDOR_PAYOUT_COMPLETED',
        category: 'PAYOUT',
        title: 'Vendor payout completed',
        message: `Vendor payout for ${payload?.service || 'service'} was completed successfully.`,
        actionUrl: '/manager/events',
        metadata: payload,
        dedupeKey: `MANAGER_VENDOR_PAYOUT:${payload?.payoutId || payload?.eventId || 'na'}:${managerAuthId}`,
      });
    }
    return;
  }

  if (eventType === 'USER_REVENUE_PAYOUT_SUCCESS') {
    const authId = String(payload?.userAuthId || '').trim();
    if (!authId) return;

    await createNotificationSafe({
      recipientAuthId: authId,
      recipientRole: 'USER',
      type: 'USER_REVENUE_PAYOUT_RECEIVED',
      category: 'PAYOUT',
      title: 'Revenue payout received',
      message: 'Generated event revenue payout has been released to your account.',
      actionUrl: '/user/my-events',
      metadata: payload,
      dedupeKey: `USER_REVENUE_PAYOUT:${payload?.payoutId || payload?.eventId || 'na'}:${authId}`,
    });
  }
};

const handleAdminEvent = async (payload) => {
  const eventType = toUpper(payload?.type || payload?.eventType);

  if (eventType === 'MANAGER_CREATED') {
    await notifyAdmins({
      type: 'MANAGER_CREATED',
      category: 'SYSTEM',
      title: 'Manager account created',
      message: `Manager ${payload?.name || payload?.email || ''} account was created.`,
      actionUrl: '/admin/team-access',
      metadata: payload,
      dedupePrefix: `MANAGER_CREATED:${payload?.email || 'na'}`,
    });
    return;
  }

  if (eventType === 'TEAM_MEMBER_BLOCKED' || eventType === 'TEAM_MEMBER_UNBLOCKED') {
    const authId = String(payload?.authId || '').trim();
    if (!authId) return;

    const isBlocked = eventType === 'TEAM_MEMBER_BLOCKED';

    await createNotificationSafe({
      recipientAuthId: authId,
      recipientRole: 'MANAGER',
      type: eventType,
      category: 'SECURITY',
      title: isBlocked ? 'Account access blocked' : 'Account access restored',
      message: isBlocked
        ? 'Your team access is blocked by admin. Contact support for details.'
        : 'Your team access has been restored by admin.',
      actionUrl: '/manager/dashboard',
      metadata: payload,
      dedupeKey: `${eventType}:${authId}:${payload?.changedAt || 'na'}`,
    });
    return;
  }
};

const handleVendorEvent = async (payload) => {
  const eventType = toUpper(payload?.type || payload?.eventType);

  if (eventType === 'VENDOR_REGISTRATION_SUBMITTED') {
    await notifyAdmins({
      type: 'VENDOR_APPLICATION_SUBMITTED',
      category: 'SYSTEM',
      title: 'Vendor application submitted',
      message: `A new vendor application (${payload?.businessName || payload?.applicationId || ''}) is pending verification.`,
      actionUrl: '/admin/vendors',
      metadata: payload,
      dedupePrefix: `VENDOR_APPLICATION:${payload?.applicationId || payload?.authId || 'na'}`,
    });
  }
};

const dispatchEventByTopic = async ({ topic, payload }) => {
  const normalizedTopic = String(topic || '').trim().toLowerCase();

  if (normalizedTopic === 'auth_events') {
    await handleAuthEvent(payload);
    return;
  }

  if (normalizedTopic === 'event_events') {
    await handleEventEvent(payload);
    return;
  }

  if (normalizedTopic === 'payment_events') {
    await handlePaymentEvent(payload);
    return;
  }

  if (normalizedTopic === 'admin_events') {
    await handleAdminEvent(payload);
    return;
  }

  if (normalizedTopic === 'vendor_events') {
    await handleVendorEvent(payload);
  }
};

const initialize = async () => {
  const maxRetries = 5;
  const retryDelay = 3000;

  for (let attempt = 1; attempt <= maxRetries; attempt += 1) {
    try {
      consumer = await kafkaConfig.createConsumer();
      await new Promise((resolve) => setTimeout(resolve, 1500));

      for (const topic of eventTopics) {
        await consumer.subscribe({ topic, fromBeginning: false });
      }

      logger.info(`Notification consumer subscribed to: ${eventTopics.join(', ')}`);
      return;
    } catch (error) {
      logger.error(`Error initializing notification Kafka consumer (attempt ${attempt}/${maxRetries})`, {
        message: error?.message || String(error),
      });

      if (attempt < maxRetries) {
        await new Promise((resolve) => setTimeout(resolve, retryDelay));
      } else {
        throw error;
      }
    }
  }
};

const startConsuming = async () => {
  if (!consumer) return;

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const payload = JSON.parse(message.value.toString());
        await dispatchEventByTopic({ topic, payload });

        logger.info('Notification consumer processed event', {
          topic,
          partition,
          key: message.key ? message.key.toString() : null,
          eventType: toUpper(payload?.type || payload?.eventType),
        });
      } catch (error) {
        logger.error('Notification consumer failed to process event', {
          topic,
          message: error?.message || String(error),
        });
      }
    },
  });

  logger.info('Notification Kafka consumer started successfully');
};

const shutdown = async () => {
  try {
    if (consumer) {
      await kafkaConfig.disconnect();
      logger.info('Notification Kafka consumer shut down gracefully');
    }
  } catch (error) {
    logger.error('Error shutting down notification Kafka consumer', {
      message: error?.message || String(error),
    });
  }
};

module.exports = {
  initialize,
  startConsuming,
  shutdown,
};
