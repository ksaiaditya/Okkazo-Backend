const User = require('../models/User');
const logger = require('../utils/logger');
const createApiError = require('../utils/ApiError');
const teamAccessEventProducer = require('../kafka/teamAccessEventProducer');

const AUTH_SERVICE_INTERNAL_URL = process.env.AUTH_SERVICE_INTERNAL_URL || 'http://localhost:8081';

const fetchAuthAccountStatuses = async (authIds = []) => {
  if (!Array.isArray(authIds) || authIds.length === 0) {
    return {};
  }

  try {
    const params = new URLSearchParams();
    authIds.forEach((id) => params.append('authIds', String(id)));

    const response = await fetch(`${AUTH_SERVICE_INTERNAL_URL}/internal/account-status?${params.toString()}`);
    if (!response.ok) {
      logger.warn('Failed to fetch auth account statuses', { status: response.status });
      return {};
    }

    const payload = await response.json().catch(() => ({}));
    return payload?.data && typeof payload.data === 'object' ? payload.data : {};
  } catch (error) {
    logger.warn('Failed to fetch auth account statuses', { message: error.message });
    return {};
  }
};

const syncAuthTeamAccessStatus = async ({ authId, changedBy, action }) => {
  const normalizedAction = String(action || '').toLowerCase();
  const endpoint = normalizedAction === 'unblock'
    ? '/internal/team-access/unblock'
    : '/internal/team-access/block';

  const params = new URLSearchParams({ authId: String(authId) });
  if (changedBy) params.append('changedBy', String(changedBy));

  const response = await fetch(`${AUTH_SERVICE_INTERNAL_URL}${endpoint}?${params.toString()}`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
  });

  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload?.message || `Failed to ${normalizedAction || 'block'} account in auth service`);
  }
};

/**
 * Create a new user
 */
const createUser = async (userData) => {
  try {
    if (!userData || Object.keys(userData).length === 0) {
      throw createApiError(400, 'User data is required');
    }

    if (!userData.authId || !userData.email) {
      throw createApiError(400, 'Auth ID and email are required');
    }

    const user = new User(userData);
    await user.save();
    logger.info(`User created: ${user.email}`);
    return user;
  } catch (error) {
    if (error.code === 11000) {
      const field = Object.keys(error.keyPattern)[0];
      throw createApiError(409, `User with this ${field} already exists`);
    }
    logger.error('Error creating user:', error);
    throw createApiError(500, 'Error creating user');
  }
};

/**
 * Get user by ID
 */
const getUserById = async (userId) => {
  try {
    if (!userId) {
      throw createApiError(400, 'User ID is required');
    }

    // Check if it's a valid MongoDB ObjectId
    if (!userId.match(/^[0-9a-fA-F]{24}$/)) {
      throw createApiError(400, 'Invalid user ID format');
    }

    const user = await User.findById(userId);
    if (!user) {
      throw createApiError(404, 'User not found');
    }
    return user;
  } catch (error) {
    if (error.statusCode) throw error;
    logger.error('Error fetching user by ID:', error);
    throw createApiError(500, 'Error fetching user');
  }
};

/**
 * Get user by authId
 */
const getUserByAuthId = async (authId) => {
  try {
    if (!authId || authId.trim() === '') {
      throw createApiError(400, 'Auth ID is required');
    }

    const user = await User.findOne({ authId: authId.trim() });
    if (!user) {
      throw createApiError(404, 'User not found');
    }
    return user;
  } catch (error) {
    if (error.statusCode) throw error;
    logger.error('Error fetching user by authId:', error);
    throw createApiError(500, 'Error fetching user');
  }
};

/**
 * Get user by email
 */
const getUserByEmail = async (email) => {
  try {
    if (!email || email.trim() === '') {
      throw createApiError(400, 'Email is required');
    }

    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(email)) {
      throw createApiError(400, 'Invalid email format');
    }

    const user = await User.findOne({ email: email.toLowerCase().trim() });
    if (!user) {
      throw createApiError(404, 'User not found');
    }
    return user;
  } catch (error) {
    if (error.statusCode) throw error;
    logger.error('Error fetching user by email:', error);
    throw createApiError(500, 'Error fetching user');
  }
};

/**
 * Update user profile
 */
const updateUser = async (userId, updateData) => {
  try {
    // Remove fields that shouldn't be updated directly
    const { authId, role, memberSince, createdAt, updatedAt, ...allowedUpdates } =
      updateData;

    const user = await User.findById(userId);
    if (!user) {
      throw createApiError(404, 'User not found');
    }

    // Strip manager-only fields if user is not a MANAGER
    if (user.role !== 'MANAGER') {
      delete allowedUpdates.department;
      delete allowedUpdates.assignedRole;
    }

    Object.assign(user, allowedUpdates);
    
    // Update profile completion status
    user.updateProfileCompletion();
    
    await user.save();
    logger.info(`User updated: ${user.email}`);
    return user;
  } catch (error) {
    if (error.statusCode) throw error;
    if (error.code === 11000) {
      const field = Object.keys(error.keyPattern)[0];
      throw createApiError(409, `User with this ${field} already exists`);
    }
    logger.error('Error updating user:', error);
    throw createApiError(500, 'Error updating user');
  }
};

/**
 * Delete user (soft delete)
 */
const deleteUser = async (userId) => {
  try {
    const user = await User.findById(userId);
    if (!user) {
      throw createApiError(404, 'User not found');
    }

    user.isActive = false;
    await user.save();
    logger.info(`User deactivated: ${user.email}`);
    return { message: 'User deactivated successfully' };
  } catch (error) {
    if (error.statusCode) throw error;
    logger.error('Error deleting user:', error);
    throw createApiError(500, 'Error deleting user');
  }
};

/**
 * Get all users with pagination and filters
 */
const getAllUsers = async (filters = {}, page = 1, limit = 10) => {
  try {
    const query = { isActive: true };

    if (filters.role) {
      query.role = filters.role.toUpperCase();
    }

    if (filters.profileIsComplete !== undefined) {
      query.profileIsComplete = filters.profileIsComplete === 'true';
    }

    if (filters.search) {
      query.$or = [
        { name: new RegExp(filters.search, 'i') },
        { fullName: new RegExp(filters.search, 'i') },
        { email: new RegExp(filters.search, 'i') },
      ];
    }

    const skip = (page - 1) * limit;

    const [users, total] = await Promise.all([
      User.find(query)
        .sort({ createdAt: -1 })
        .skip(skip)
        .limit(Number(limit))
        .lean(),
      User.countDocuments(query),
    ]);

    return {
      users,
      pagination: {
        currentPage: Number(page),
        totalPages: Math.ceil(total / limit),
        totalUsers: total,
        limit: Number(limit),
      },
    };
  } catch (error) {
    logger.error('Error fetching users:', error);
    throw createApiError(500, 'Error fetching users');
  }
};

/**
 * Get platform users with auth-service account status enrichment
 */
const getPlatformUsers = async (filters = {}, page = 1, limit = 10) => {
  try {
    const query = {};

    if (filters.role) {
      query.role = filters.role.toUpperCase();
    }

    if (filters.search) {
      query.$or = [
        { name: new RegExp(filters.search, 'i') },
        { fullName: new RegExp(filters.search, 'i') },
        { email: new RegExp(filters.search, 'i') },
      ];
    }

    const skip = (page - 1) * limit;

    const [users, total] = await Promise.all([
      User.find(query)
        .sort({ createdAt: -1 })
        .skip(skip)
        .limit(Number(limit))
        .lean(),
      User.countDocuments(query),
    ]);

    const authIds = users.map((user) => user?.authId).filter(Boolean);
    const statusByAuthId = await fetchAuthAccountStatuses(authIds);

    const usersWithStatus = users.map((user) => {
      const authStatusRaw = statusByAuthId[String(user.authId)] || null;
      const authStatus = authStatusRaw ? String(authStatusRaw).toUpperCase() : null;

      let accountStatus = 'SUSPENDED';
      if (authStatus === 'BLOCKED') {
        accountStatus = 'BLOCKED';
      } else if (authStatus === 'ACTIVE') {
        accountStatus = 'ACTIVE';
      } else if (authStatus === 'UNVERIFIED' || authStatus === 'DELETED') {
        accountStatus = 'SUSPENDED';
      } else if (user.isActive === false) {
        accountStatus = 'BLOCKED';
      } else if (user.lastLogin) {
        accountStatus = 'ACTIVE';
      }

      return {
        ...user,
        accountStatus,
      };
    });

    return {
      users: usersWithStatus,
      pagination: {
        currentPage: Number(page),
        totalPages: Math.ceil(total / limit),
        totalUsers: total,
        limit: Number(limit),
      },
    };
  } catch (error) {
    logger.error('Error fetching platform users:', error);
    throw createApiError(500, 'Error fetching platform users');
  }
};

/**
 * Update user's last login timestamp
 */
const updateLastLogin = async (authId) => {
  try {
    if (!authId || authId.trim() === '') {
      throw createApiError(400, 'Auth ID is required');
    }

    const user = await User.findOneAndUpdate(
      { authId: authId.trim() },
      { lastLogin: new Date() },
      { new: true }
    );

    if (!user) {
      logger.warn(`User not found for lastLogin update: ${authId}`);
      throw createApiError(404, 'User not found');
    }

    logger.info(`Last login updated for user: ${user.email}`);
    return user;
  } catch (error) {
    if (error.statusCode) throw error;
    logger.error('Error updating last login:', error);
    throw createApiError(500, 'Error updating last login');
  }
};

/**
 * Update user's role
 */
const updateUserRole = async (authId, newRole) => {
  try {
    if (!authId || authId.trim() === '') {
      throw createApiError(400, 'Auth ID is required');
    }

    if (!newRole) {
      throw createApiError(400, 'New role is required');
    }

    const validRoles = ['USER', 'VENDOR', 'ADMIN', 'MANAGER'];
    if (!validRoles.includes(newRole.toUpperCase())) {
      throw createApiError(400, `Invalid role. Must be one of: ${validRoles.join(', ')}`);
    }

    const user = await User.findOneAndUpdate(
      { authId: authId.trim() },
      { role: newRole.toUpperCase() },
      { new: true }
    );

    if (!user) {
      logger.warn(`User not found for role update: ${authId}`);
      throw createApiError(404, 'User not found');
    }

    logger.info(`Role updated for user ${user.email}: ${newRole}`);
    return user;
  } catch (error) {
    if (error.statusCode) throw error;
    logger.error('Error updating user role:', error);
    throw createApiError(500, 'Error updating user role');
  }
};

/**
 * Get team access data for admin page
 */
const getTeamAccessData = async ({ search = '', page = 1, limit = 10 } = {}) => {
  try {
    const safePage = Math.max(Number(page) || 1, 1);
    const safeLimit = Math.min(Math.max(Number(limit) || 10, 1), 50);
    const skip = (safePage - 1) * safeLimit;

    const baseQuery = {
      role: { $in: ['ADMIN', 'MANAGER'] },
    };

    if (search && search.trim() !== '') {
      const regex = new RegExp(search.trim(), 'i');
      baseQuery.$or = [
        { name: regex },
        { fullName: regex },
        { email: regex },
        { assignedRole: regex },
      ];
    }

    const [users, total, totalAdmins, totalManagers, activeMembers, pendingInvites] = await Promise.all([
      User.find(baseQuery)
        .sort({ lastLogin: -1, createdAt: -1 })
        .skip(skip)
        .limit(safeLimit)
        .lean(),
      User.countDocuments(baseQuery),
      User.countDocuments({ role: 'ADMIN' }),
      User.countDocuments({ role: 'MANAGER' }),
      User.countDocuments({ role: { $in: ['ADMIN', 'MANAGER'] }, isActive: true }),
      User.countDocuments({ role: { $in: ['ADMIN', 'MANAGER'] }, isActive: true, lastLogin: null }),
    ]);

    const members = users.map((user) => ({
      id: user._id,
      authId: user.authId,
      name: user.name,
      email: user.email,
      role: user.role,
      assignedRole: user.assignedRole,
      department: user.department,
      isActive: user.isActive,
      status: !user.isActive ? 'BLOCKED' : (!user.lastLogin ? 'UNVERIFIED' : 'ACTIVE'),
      lastActive: user.lastLogin,
      access: user.role === 'ADMIN' ? 'Full Access' : user.assignedRole || 'Manager Access',
    }));

    return {
      members,
      stats: {
        totalMembers: totalAdmins + totalManagers,
        admins: totalAdmins,
        managers: totalManagers,
        activeMembers,
        pendingInvites,
      },
      pagination: {
        page: safePage,
        limit: safeLimit,
        total,
        totalPages: Math.ceil(total / safeLimit),
      },
    };
  } catch (error) {
    logger.error('Error fetching team access data:', error);
    throw createApiError(500, 'Error fetching team access data');
  }
};

/**
 * Block a team member (Admin action)
 */
const blockTeamMember = async ({ authId, requestedByAuthId }) => {
  try {
    if (!authId || authId.trim() === '') {
      throw createApiError(400, 'Auth ID is required');
    }

    if (authId === requestedByAuthId) {
      throw createApiError(400, 'You cannot block your own account');
    }

    const user = await User.findOne({ authId: authId.trim() });
    if (!user) {
      throw createApiError(404, 'Team member not found');
    }

    if (!['ADMIN', 'MANAGER'].includes(user.role)) {
      throw createApiError(400, 'Only admin/manager team members can be blocked from Team Access');
    }

    if (!user.isActive) {
      return user;
    }

    const previousIsActive = user.isActive;
    user.isActive = false;
    await user.save();

    try {
      await syncAuthTeamAccessStatus({
        authId: user.authId,
        changedBy: requestedByAuthId,
        action: 'block',
      });
    } catch (syncError) {
      user.isActive = previousIsActive;
      await user.save();
      logger.error('Failed to synchronize TEAM_MEMBER_BLOCKED with auth service', syncError);
      throw createApiError(502, 'Failed to synchronize block status with auth service');
    }

    try {
      await teamAccessEventProducer.publishTeamMemberBlocked({
        authId: user.authId,
        email: user.email,
        changedBy: requestedByAuthId,
      });
    } catch (eventError) {
      logger.warn('Failed to publish TEAM_MEMBER_BLOCKED event after successful direct sync', eventError);
    }

    logger.info('Team member blocked successfully', {
      authId: user.authId,
      email: user.email,
      blockedBy: requestedByAuthId,
    });

    return user;
  } catch (error) {
    if (error.statusCode) throw error;
    logger.error('Error blocking team member:', error);
    throw createApiError(500, 'Error blocking team member');
  }
};

/**
 * Unblock a team member (Admin action)
 */
const unblockTeamMember = async ({ authId, requestedByAuthId }) => {
  try {
    if (!authId || authId.trim() === '') {
      throw createApiError(400, 'Auth ID is required');
    }

    const user = await User.findOne({ authId: authId.trim() });
    if (!user) {
      throw createApiError(404, 'Team member not found');
    }

    if (!['ADMIN', 'MANAGER'].includes(user.role)) {
      throw createApiError(400, 'Only admin/manager team members can be unblocked from Team Access');
    }

    if (user.isActive) {
      return user;
    }

    const previousIsActive = user.isActive;
    user.isActive = true;
    await user.save();

    try {
      await syncAuthTeamAccessStatus({
        authId: user.authId,
        changedBy: requestedByAuthId,
        action: 'unblock',
      });
    } catch (syncError) {
      user.isActive = previousIsActive;
      await user.save();
      logger.error('Failed to synchronize TEAM_MEMBER_UNBLOCKED with auth service', syncError);
      throw createApiError(502, 'Failed to synchronize unblock status with auth service');
    }

    try {
      await teamAccessEventProducer.publishTeamMemberUnblocked({
        authId: user.authId,
        email: user.email,
        changedBy: requestedByAuthId,
      });
    } catch (eventError) {
      logger.warn('Failed to publish TEAM_MEMBER_UNBLOCKED event after successful direct sync', eventError);
    }

    logger.info('Team member unblocked successfully', {
      authId: user.authId,
      email: user.email,
      unblockedBy: requestedByAuthId,
    });

    return user;
  } catch (error) {
    if (error.statusCode) throw error;
    logger.error('Error unblocking team member:', error);
    throw createApiError(500, 'Error unblocking team member');
  }
};

/**
 * Get user statistics
 */
const getUserStats = async () => {
  try {
    const [totalUsers, activeUsers, byRole, completedProfiles] = await Promise.all([
      User.countDocuments(),
      User.countDocuments({ isActive: true }),
      User.aggregate([
        { $group: { _id: '$role', count: { $sum: 1 } } },
      ]),
      User.countDocuments({ profileIsComplete: true }),
    ]);

    return {
      totalUsers,
      activeUsers,
      inactiveUsers: totalUsers - activeUsers,
      completedProfiles,
      incompleteProfiles: totalUsers - completedProfiles,
      byRole: byRole.reduce((acc, item) => {
        acc[item._id] = item.count;
        return acc;
      }, {}),
    };
  } catch (error) {
    logger.error('Error fetching user stats:', error);
    throw createApiError(500, 'Error fetching user statistics');
  }
};

module.exports = {
  createUser,
  getUserById,
  getUserByAuthId,
  getUserByEmail,
  updateUser,
  deleteUser,
  getAllUsers,
  getPlatformUsers,
  updateLastLogin,
  updateUserRole,
  getTeamAccessData,
  blockTeamMember,
  unblockTeamMember,
  getUserStats,
};
