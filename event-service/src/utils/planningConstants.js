const CATEGORY = {
  PUBLIC: 'public',
  PRIVATE: 'private',
};

const PRIVATE_EVENT_TYPES = [
  'Birthday',
  'Wedding',
  'Anniversary',
  'Party',
  'Dinner',
  'Other',
];

const PUBLIC_EVENT_TYPES = [
  'Concert',
  'Festival',
  'Exhibition',
  'Workshop',
  'Seminar',
  'Other',
];

const SERVICE_OPTIONS = [
  'Venue',
  'Catering & Drinks',
  'Photography',
  'Videography',
  'Decor & Styling',
  'Entertainment & Artists',
  'Makeup & Grooming',
  'Invitations & Printing',
  'Sound & Lighting',
  'Equipment Rental',
  'Security & Safety',
  'Transportation',
  'Live Streaming & Media',
  'Cake & Desserts',
  'Other',
];

const PUBLIC_PROMOTION_OPTIONS = [
  'featured placement',
  'email blast',
  'advance analysis',
  'Social Synergy',
];

const STATUS = {
  PAYMENT_PENDING: 'PAYMENT PENDING',
  IMMEDIATE_ACTION: 'IMMEDIATE ACTION',
  PENDING_APPROVAL: 'PENDING APPROVAL',
  APPROVED: 'APPROVED',
  CONFIRMED: 'CONFIRMED',
  REJECTED: 'REJECTED',
  COMPLETED: 'COMPLETED',
  VENDOR_PAYMENT_PENDING: 'VENDOR PAYMENT PENDING',
  CLOSED: 'CLOSED',
};

const STATUS_VALUES = Object.values(STATUS);

const TERMINAL_STATUSES = [
  STATUS.REJECTED,
  STATUS.COMPLETED,
  STATUS.VENDOR_PAYMENT_PENDING,
  STATUS.CLOSED,
];

const USER_HIDDEN_STATUSES = [
  STATUS.VENDOR_PAYMENT_PENDING,
  STATUS.CLOSED,
];

module.exports = {
  CATEGORY,
  PRIVATE_EVENT_TYPES,
  PUBLIC_EVENT_TYPES,
  SERVICE_OPTIONS,
  PUBLIC_PROMOTION_OPTIONS,
  STATUS,
  STATUS_VALUES,
  TERMINAL_STATUSES,
  USER_HIDDEN_STATUSES,
};
