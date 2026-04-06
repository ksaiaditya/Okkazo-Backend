const { v4: uuidv4 } = require('uuid');
const Notification = require('../models/Notification');
const createApiError = require('../utils/ApiError');

const normalizeRole = (value) => String(value || '').trim().toUpperCase();
const normalizeType = (value) => String(value || '').trim().toUpperCase();
const normalizeCategory = (value) => String(value || '').trim().toUpperCase();

const createNotification = async ({
  recipientAuthId,
  recipientRole,
  type,
  category,
  title,
  message,
  actionUrl = null,
  metadata = {},
  dedupeKey = null,
  createdAt = null,
}) => {
  const authId = String(recipientAuthId || '').trim();
  if (!authId) {
    throw createApiError(400, 'recipientAuthId is required');
  }

  const safeType = normalizeType(type || 'INFO');
  const safeCategory = normalizeCategory(category || 'SYSTEM');
  const safeRole = normalizeRole(recipientRole || 'USER');

  const payload = {
    notificationId: uuidv4(),
    recipientAuthId: authId,
    recipientRole: safeRole,
    type: safeType,
    category: safeCategory,
    title: String(title || '').trim() || 'Notification',
    message: String(message || '').trim() || 'You have a new notification.',
    actionUrl: actionUrl ? String(actionUrl).trim() : null,
    metadata: metadata && typeof metadata === 'object' ? metadata : {},
    dedupeKey: dedupeKey ? String(dedupeKey).trim() : null,
    unread: true,
    readAt: null,
  };

  if (createdAt) {
    const when = new Date(createdAt);
    if (!Number.isNaN(when.getTime())) {
      payload.createdAt = when;
      payload.updatedAt = when;
    }
  }

  if (payload.dedupeKey) {
    const existing = await Notification.findOne({
      recipientAuthId: payload.recipientAuthId,
      dedupeKey: payload.dedupeKey,
    }).lean();

    if (existing) {
      return existing;
    }
  }

  try {
    const created = await Notification.create(payload);
    return created.toObject();
  } catch (error) {
    if (error?.code === 11000 && payload.dedupeKey) {
      const existing = await Notification.findOne({
        recipientAuthId: payload.recipientAuthId,
        dedupeKey: payload.dedupeKey,
      }).lean();
      if (existing) return existing;
    }
    throw error;
  }
};

const createNotificationsBulk = async (items = []) => {
  if (!Array.isArray(items) || items.length === 0) return [];

  const created = [];
  for (const item of items) {
    try {
      const row = await createNotification(item);
      created.push(row);
    } catch (error) {
      // Ignore one-off failures in bulk operations and continue.
      // Caller can inspect logs for details.
    }
  }

  return created;
};

const listNotificationsForUser = async ({ authId, tab = 'all', search = '', page = 1, limit = 50 }) => {
  const userAuthId = String(authId || '').trim();
  if (!userAuthId) {
    throw createApiError(400, 'authId is required');
  }

  const safePage = Math.max(1, Number(page) || 1);
  const safeLimit = Math.min(100, Math.max(1, Number(limit) || 50));
  const skip = (safePage - 1) * safeLimit;

  const query = {
    recipientAuthId: userAuthId,
  };

  const normalizedTab = String(tab || 'all').trim().toLowerCase();
  if (normalizedTab === 'unread') {
    query.unread = true;
  } else if (normalizedTab === 'system') {
    query.category = { $in: ['SYSTEM', 'SECURITY'] };
  } else if (normalizedTab === 'promotions') {
    query.category = 'PROMOTION';
  }

  const q = String(search || '').trim();
  if (q) {
    query.$or = [
      { title: new RegExp(q, 'i') },
      { message: new RegExp(q, 'i') },
      { type: new RegExp(q, 'i') },
      { category: new RegExp(q, 'i') },
    ];
  }

  const [items, total] = await Promise.all([
    Notification.find(query)
      .sort({ createdAt: -1 })
      .skip(skip)
      .limit(safeLimit)
      .lean(),
    Notification.countDocuments(query),
  ]);

  return {
    notifications: items,
    pagination: {
      currentPage: safePage,
      totalPages: Math.max(1, Math.ceil(total / safeLimit)),
      total,
      limit: safeLimit,
    },
  };
};

const getUnreadCount = async ({ authId }) => {
  const userAuthId = String(authId || '').trim();
  if (!userAuthId) {
    throw createApiError(400, 'authId is required');
  }

  const unreadCount = await Notification.countDocuments({
    recipientAuthId: userAuthId,
    unread: true,
  });

  return { unreadCount };
};

const markNotificationAsRead = async ({ authId, notificationId }) => {
  const userAuthId = String(authId || '').trim();
  const id = String(notificationId || '').trim();

  if (!userAuthId || !id) {
    throw createApiError(400, 'authId and notificationId are required');
  }

  const updated = await Notification.findOneAndUpdate(
    {
      recipientAuthId: userAuthId,
      notificationId: id,
    },
    {
      $set: {
        unread: false,
        readAt: new Date(),
      },
    },
    { new: true }
  ).lean();

  if (!updated) {
    throw createApiError(404, 'Notification not found');
  }

  return updated;
};

const markAllNotificationsAsRead = async ({ authId }) => {
  const userAuthId = String(authId || '').trim();
  if (!userAuthId) {
    throw createApiError(400, 'authId is required');
  }

  const result = await Notification.updateMany(
    {
      recipientAuthId: userAuthId,
      unread: true,
    },
    {
      $set: {
        unread: false,
        readAt: new Date(),
      },
    }
  );

  return {
    updatedCount: Number(result.modifiedCount || 0),
  };
};

module.exports = {
  createNotification,
  createNotificationsBulk,
  listNotificationsForUser,
  getUnreadCount,
  markNotificationAsRead,
  markAllNotificationsAsRead,
};
