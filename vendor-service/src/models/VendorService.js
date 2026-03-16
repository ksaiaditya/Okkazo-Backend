const mongoose = require('mongoose');

/**
 * VendorService model
 *
 * Stores services created by an approved vendor on the Service Management page.
 * The `details` field is a flexible Mixed type that holds category-specific
 * fields (e.g. catering: tier, items; venues: capacity, location; etc.).
 */
const vendorServiceSchema = new mongoose.Schema(
  {
    // Reference to the vendor (matches VendorApplication.authId)
    authId: {
      type: String,
      required: [true, 'Auth ID is required'],
      index: true,
    },

    // Denormalised for quick search without a join
    businessName: {
      type: String,
      required: [true, 'Business name is required'],
      trim: true,
      index: true,
    },

    // Must match the enum in VendorApplication.serviceCategory
    serviceCategory: {
      type: String,
      required: [true, 'Service category is required'],
      enum: [
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
      ],
      index: true,
    },

    // Frontend category id (e.g. "catering", "venues", "photography")
    categoryId: {
      type: String,
      required: [true, 'Category ID is required'],
      trim: true,
    },

    // Human-readable service / package name
    name: {
      type: String,
      required: [true, 'Service name is required'],
      trim: true,
      maxlength: [200, 'Name cannot exceed 200 characters'],
    },

    // Base price in INR (numeric, stored without currency symbol)
    price: {
      type: Number,
      required: [true, 'Price is required'],
      min: [0, 'Price cannot be negative'],
    },

    // Optional tier label (Economy, Standard, Luxury, Silver, Gold, etc.)
    tier: {
      type: String,
      trim: true,
      default: null,
    },

    // Optional short description
    description: {
      type: String,
      trim: true,
      maxlength: [2000, 'Description cannot exceed 2000 characters'],
      default: null,
    },

    // Category-specific extra fields stored as a flexible object
    // e.g. { items: [...], capacity: 500, location: "Mumbai" }
    details: {
      type: mongoose.Schema.Types.Mixed,
      default: {},
    },

    // Geo coordinates – sourced from the vendor's approved application
    latitude: {
      type: Number,
      default: null,
    },
    longitude: {
      type: Number,
      default: null,
    },

    status: {
      type: String,
      enum: ['Active', 'Inactive'],
      default: 'Active',
    },
  },
  {
    timestamps: true,
    versionKey: false,
  }
);

// 2dsphere index for geo-based queries
vendorServiceSchema.index({ latitude: 1, longitude: 1 });
vendorServiceSchema.index({ serviceCategory: 1, businessName: 1 });
vendorServiceSchema.index({ authId: 1, status: 1 });

// Enforce: a vendor cannot have multiple services with the same tier.
// Partial index so tier=null (or missing) does not collide.
vendorServiceSchema.index(
  { authId: 1, tier: 1 },
  {
    unique: true,
    partialFilterExpression: {
      tier: { $type: 'string', $ne: '' },
    },
  }
);

const VendorService = mongoose.model('VendorService', vendorServiceSchema);

module.exports = VendorService;
