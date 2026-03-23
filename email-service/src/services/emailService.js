const handlebars = require('handlebars');
const fs = require('fs').promises;
const path = require('path');
const emailConfig = require('../config/email');
const logger = require('../utils/logger');

// Module state
let transporter = null;
const templatesCache = {};
const maxRetries = parseInt(process.env.MAX_RETRY_ATTEMPTS, 10) || 3;
const retryDelay = parseInt(process.env.RETRY_DELAY, 10) || 5000;

const initialize = async () => {
  transporter = emailConfig.getTransporter();
  await emailConfig.verifyConnection();
};

/**
 * Load and compile email template
 */
const loadTemplate = async (templateName) => {
  try {
    // Check cache first
    if (templatesCache[templateName]) {
      return templatesCache[templateName];
    }

    const templatePath = path.join(
      __dirname,
      '../templates',
      `${templateName}.html`
    );

    const templateContent = await fs.readFile(templatePath, 'utf-8');
    const compiledTemplate = handlebars.compile(templateContent);

    // Cache the compiled template
    templatesCache[templateName] = compiledTemplate;

    return compiledTemplate;
  } catch (error) {
    logger.error(`Error loading template ${templateName}:`, error);
    throw new Error(`Failed to load email template: ${templateName}`);
  }
};

/**
 * Delay helper for retry logic
 */
const delay = (ms) => {
  return new Promise((resolve) => setTimeout(resolve, ms));
};

/**
 * Send email with retry logic
 */
const sendEmail = async (to, subject, html, retryCount = 0) => {
  try {
    if (!to || !subject || !html) {
      throw new Error('Missing required email parameters');
    }

    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(to)) {
      throw new Error(`Invalid email address: ${to}`);
    }

    const mailOptions = {
      from: {
        name: process.env.FROM_NAME || 'Okkazo Platform',
        address: process.env.FROM_EMAIL || 'noreply@okkazo.com',
      },
      to: to,
      subject: subject,
      html: html,
    };

    const info = await transporter.sendMail(mailOptions);

    logger.info('Email sent successfully', {
      to,
      subject,
      messageId: info.messageId,
    });

    return {
      success: true,
      messageId: info.messageId,
    };
  } catch (error) {
    logger.error(`Email sending failed (attempt ${retryCount + 1}):`, error);

    // Retry logic
    if (retryCount < maxRetries) {
      logger.info(`Retrying email send in ${retryDelay}ms...`);
      await delay(retryDelay);
      return sendEmail(to, subject, html, retryCount + 1);
    }

    logger.error(`Failed to send email after ${maxRetries} attempts`);
    throw error;
  }
};

/**
 * Send email verification
 */
const sendVerificationEmail = async (email, verificationToken, authId) => {
  try {
    if (!email || !verificationToken) {
      throw new Error('Email and verification token are required');
    }

    const template = await loadTemplate('verification');
    const verificationLink = `${process.env.VERIFICATION_URL}?token=${verificationToken}`;

    const html = template({
      verificationLink,
      email,
      platformName: 'Okkazo',
      supportEmail: process.env.FROM_EMAIL,
    });

    await sendEmail(email, 'Verify Your Email - Okkazo', html);

    logger.info('Verification email sent', { email, authId });
  } catch (error) {
    logger.error('Error sending verification email:', error);
    throw error;
  }
};

/**
 * Send password reset email
 */
const sendPasswordResetEmail = async (email, resetToken, authId) => {
  try {
    if (!email || !resetToken) {
      throw new Error('Email and reset token are required');
    }

    const template = await loadTemplate('password-reset');
    const resetLink = `${process.env.RESET_PASSWORD_URL}?token=${resetToken}`;

    const html = template({
      resetLink,
      email,
      platformName: 'Okkazo',
      supportEmail: process.env.FROM_EMAIL,
      expiryTime: '1 hour',
    });

    await sendEmail(email, 'Reset Your Password - Okkazo', html);

    logger.info('Password reset email sent', { email, authId });
  } catch (error) {
    logger.error('Error sending password reset email:', error);
    throw error;
  }
};

/**
 * Send welcome email
 */
const sendWelcomeEmail = async (email, username) => {
  try {
    if (!email || !username) {
      throw new Error('Email and username are required');
    }

    const template = await loadTemplate('welcome');

    const html = template({
      username,
      platformName: 'Okkazo',
      platformUrl: process.env.FRONTEND_URL,
      supportEmail: process.env.FROM_EMAIL,
    });

    await sendEmail(email, 'Welcome to Okkazo!', html);

    logger.info('Welcome email sent', { email, username });
  } catch (error) {
    logger.error('Error sending welcome email:', error);
    throw error;
  }
};

/**
 * Send test email
 */
const sendTestEmail = async (to) => {
  try {
    const html = `
      <html>
        <body style="font-family: Arial, sans-serif; padding: 20px; background-color: #f4f4f4;">
          <div style="max-width: 600px; margin: 0 auto; background-color: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
            <h1 style="color: #6366f1; margin-bottom: 20px;">🎉 Test Email</h1>
            <p style="color: #333; font-size: 16px; line-height: 1.6;">
              This is a test email from <strong>Okkazo Platform</strong> Email Service.
            </p>
            <p style="color: #666; font-size: 14px; margin-top: 20px;">
              If you received this email, your SMTP configuration is working correctly!
            </p>
            <div style="margin-top: 30px; padding: 20px; background-color: #f0f9ff; border-left: 4px solid #6366f1; border-radius: 4px;">
              <p style="margin: 0; color: #333; font-weight: bold;">✅ Configuration Status</p>
              <ul style="margin: 10px 0 0 0; padding-left: 20px; color: #666;">
                <li>SMTP Server: Connected</li>
                <li>Email Service: Running</li>
                <li>Templates: Loaded</li>
                <li>Kafka: Listening</li>
              </ul>
            </div>
            <p style="color: #999; font-size: 12px; margin-top: 30px; text-align: center;">
              Sent at: ${new Date().toLocaleString()}
            </p>
          </div>
        </body>
      </html>
    `;

    return await sendEmail(to, 'Test Email - Okkazo Platform', html);
  } catch (error) {
    logger.error('Error sending test email:', error);
    throw error;
  }
};

/**
 * Send vendor account created email with password setup link
 */
const sendVendorAccountCreatedEmail = async (email, passwordResetToken, businessName, applicationId, authId) => {
  try {
    if (!email || !passwordResetToken || !businessName || !applicationId) {
      throw new Error('Email, password reset token, business name, and application ID are required');
    }

    const template = await loadTemplate('vendor-registration');
    const setPasswordLink = `${process.env.RESET_PASSWORD_URL}?token=${passwordResetToken}`;

    const html = template({
      setPasswordLink,
      businessName,
      applicationId,
      email,
      platformName: 'Okkazo',
      supportEmail: process.env.FROM_EMAIL,
      estimatedTime: '2-3 business days',
      expiryTime: '7 days',
    });

    await sendEmail(email, 'Vendor Application Received - Set Your Password', html);

    logger.info('Vendor account created email sent', { email, authId, applicationId });
  } catch (error) {
    logger.error('Error sending vendor account created email:', error);
    throw error;
  }
};

/**
 * Send manager account created email with password setup link
 */
const sendManagerAccountCreatedEmail = async (email, passwordResetToken, name, department, assignedRole, authId) => {
  try {
    if (!email || !passwordResetToken || !name) {
      throw new Error('Email, password reset token, and name are required');
    }

    const template = await loadTemplate('manager-invitation');
    const setPasswordLink = `${process.env.RESET_PASSWORD_URL}?token=${passwordResetToken}`;

    const html = template({
      setPasswordLink,
      name,
      department: department || 'Not assigned',
      assignedRole: assignedRole || 'MANAGER',
      email,
      platformName: 'Okkazo',
      supportEmail: process.env.FROM_EMAIL,
      expiryTime: '7 days',
    });

    await sendEmail(email, 'You\'ve Been Invited as a Manager - Set Your Password', html);

    logger.info('Manager account created email sent', { email, authId, department });
  } catch (error) {
    logger.error('Error sending manager account created email:', error);
    throw error;
  }
};

/**
 * Send payment success email with event details
 */
const sendPaymentSuccessEmail = async (email, details) => {
  try {
    if (!email) {
      throw new Error('Email is required');
    }

    const {
      recipientName,
      eventId,
      eventTitle,
      eventLocation,
      eventStatus,
      amount,
      currency,
      transactionId,
      paidAt,
    } = details || {};

    if (!eventId) {
      throw new Error('Event ID is required');
    }

    const template = await loadTemplate('payment-success');

    const amountNumber = amount === null || amount === undefined ? null : Number(amount);
    const amountRupees = Number.isFinite(amountNumber)
      ? (amountNumber % 100 === 0 ? String(amountNumber / 100) : (amountNumber / 100).toFixed(2))
      : null;

    const currencyCode = (currency || 'INR').toString().toUpperCase();
    const currencySymbol = currencyCode === 'INR' ? '₹' : '';

    const html = template({
      recipientName: recipientName || 'there',
      eventId,
      eventTitle: eventTitle || 'Event',
      eventLocation: eventLocation || 'TBA',
      eventStatus: eventStatus || 'CONFIRMED',
      amountRupees,
      currency: currencyCode,
      currencySymbol,
      transactionId,
      paidAt,
      platformName: 'Okkazo',
      supportEmail: process.env.FROM_EMAIL,
    });

    await sendEmail(email, `Payment Successful - ${eventTitle || eventId}`, html);

    logger.info('Payment success email sent', { email, eventId, transactionId });
  } catch (error) {
    logger.error('Error sending payment success email:', error);
    throw error;
  }
};

/**
 * Send email to client when a vendor rejects and alternatives are available.
 */
const sendVendorRejectedAlternativesEmail = async (email, details) => {
  try {
    const {
      recipientName,
      eventId,
      eventTitle,
      eventDate,
      eventLocation,
      serviceLabel,
      rejectionReason,
      options,
    } = details || {};

    if (!email || !eventId || !serviceLabel) {
      throw new Error('email, eventId, and serviceLabel are required');
    }

    const template = await loadTemplate('vendor-rejected-alternatives');

    const html = template({
      recipientName: recipientName || 'there',
      eventId,
      eventTitle: eventTitle || 'Event',
      eventDate: eventDate || null,
      eventLocation: eventLocation || 'TBA',
      serviceLabel,
      rejectionReason: rejectionReason || null,
      options: Array.isArray(options) ? options : [],
      platformName: 'Okkazo',
      supportEmail: process.env.FROM_EMAIL,
    });

    await sendEmail(email, `Vendor unavailable for ${serviceLabel} - Choose an alternative`, html);
    logger.info('Vendor rejected alternatives email sent', { email, eventId, serviceLabel });
  } catch (error) {
    logger.error('Error sending vendor rejected alternatives email:', error);
    throw error;
  }
};

/**
 * Send finalized quotation email to a user (commission hidden; prices are client totals).
 */
const sendPlanningQuoteLockedUserEmail = async (email, details) => {
  try {
    if (!email) throw new Error('Email is required');

    const {
      recipientName,
      eventId,
      eventTitle,
      eventDate,
      eventLocation,
      version,
      items,
      promotionsTotal,
      grandTotal,
    } = details || {};

    if (!eventId) throw new Error('eventId is required');
    if (!eventTitle) throw new Error('eventTitle is required');

    const template = await loadTemplate('quote-locked-user');
    const html = template({
      recipientName: recipientName || 'there',
      eventId,
      eventTitle,
      eventDate: eventDate || null,
      eventLocation: eventLocation || null,
      version: version || 1,
      items: Array.isArray(items) ? items : [],
      promotionsTotal: promotionsTotal || null,
      grandTotal: grandTotal || null,
      platformName: 'Okkazo',
      supportEmail: process.env.FROM_EMAIL,
    });

    await sendEmail(email, `Final Quotation - ${eventTitle}`, html);
    logger.info('Planning quote locked user email sent', { email, eventId, version });
  } catch (error) {
    logger.error('Error sending planning quote locked user email:', error);
    throw error;
  }
};

/**
 * Send finalized quotation email to a vendor (shows their services + commission/service-charge breakdown).
 */
const sendPlanningQuoteLockedVendorEmail = async (email, details) => {
  try {
    if (!email) throw new Error('Email is required');

    const {
      recipientName,
      eventId,
      eventTitle,
      eventDate,
      eventLocation,
      version,
      items,
      vendorSubtotal,
      serviceChargeTotal,
      clientTotal,
    } = details || {};

    if (!eventId) throw new Error('eventId is required');
    if (!eventTitle) throw new Error('eventTitle is required');

    const template = await loadTemplate('quote-locked-vendor');
    const html = template({
      recipientName: recipientName || 'there',
      eventId,
      eventTitle,
      eventDate: eventDate || null,
      eventLocation: eventLocation || null,
      version: version || 1,
      items: Array.isArray(items) ? items : [],
      vendorSubtotal: vendorSubtotal || null,
      serviceChargeTotal: serviceChargeTotal || null,
      clientTotal: clientTotal || null,
      platformName: 'Okkazo',
      supportEmail: process.env.FROM_EMAIL,
    });

    await sendEmail(email, `Final Quotation (Vendor) - ${eventTitle}`, html);
    logger.info('Planning quote locked vendor email sent', { email, eventId, version });
  } catch (error) {
    logger.error('Error sending planning quote locked vendor email:', error);
    throw error;
  }
};

module.exports = {
  initialize,
  loadTemplate,
  sendEmail,
  sendVerificationEmail,
  sendPasswordResetEmail,
  sendWelcomeEmail,
  sendTestEmail,
  sendVendorAccountCreatedEmail,
  sendManagerAccountCreatedEmail,
  sendPaymentSuccessEmail,
  sendVendorRejectedAlternativesEmail,
  sendPlanningQuoteLockedUserEmail,
  sendPlanningQuoteLockedVendorEmail,
};
