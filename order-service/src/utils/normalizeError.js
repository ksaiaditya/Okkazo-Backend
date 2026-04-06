const createApiError = require('./ApiError');

const safeString = (value) => {
  if (value === undefined || value === null) return '';
  if (typeof value === 'string') return value;
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
};

const isAxiosLikeError = (err) => {
  return Boolean(err && (err.isAxiosError || err.response || err.request));
};

const normalizeAxiosError = (err, options = {}) => {
  const upstreamName = options.upstreamName || 'upstream service';

  const status = err?.response?.status;
  const upstreamMessage =
    err?.response?.data?.message ||
    err?.response?.data?.error ||
    err?.response?.statusText ||
    err?.message;

  // If upstream returned a meaningful status code, preserve it.
  if (status && Number.isInteger(status)) {
    return createApiError(status, upstreamMessage || `${upstreamName} request failed`);
  }

  // No response usually means timeout / network / DNS.
  const timeout = err?.code === 'ECONNABORTED' || /timeout/i.test(String(err?.message || ''));
  if (timeout) {
    return createApiError(504, `${upstreamName} timed out`);
  }

  return createApiError(502, upstreamMessage || `${upstreamName} request failed`);
};

const isRazorpayLikeError = (err) => {
  return Boolean(err && (err.statusCode || err.error));
};

const normalizeRazorpayError = (err, options = {}) => {
  const defaultStatusCode = options.defaultStatusCode || 422;
  const defaultMessage = options.defaultMessage || 'Payment provider error';

  const responseStatus = Number(err?.response?.status);
  const responseData = err?.response?.data;
  const responseError = responseData?.error && typeof responseData.error === 'object'
    ? responseData.error
    : null;

  const statusCode =
    (Number.isFinite(responseStatus) ? responseStatus : null)
    || Number(err?.statusCode)
    || Number(err?.error?.statusCode)
    || defaultStatusCode;

  const description =
    responseError?.description ||
    responseError?.reason ||
    responseError?.message ||
    responseData?.message ||
    err?.error?.description ||
    err?.error?.message ||
    err?.message ||
    '';

  const code =
    responseError?.code ||
    responseData?.code ||
    err?.error?.code ||
    err?.code;
  const message = [description, code ? `(${safeString(code)})` : ''].filter(Boolean).join(' ');

  const normalizedDescription = String(description || '').trim();
  const isRouteNotEnabled = /route feature not enabled/i.test(normalizedDescription);

  const finalStatusCode = isRouteNotEnabled
    ? 409
    : (Number.isFinite(statusCode) ? statusCode : defaultStatusCode);

  const finalMessage = isRouteNotEnabled
    ? 'Razorpay Route is not enabled for this merchant account. Please enable Route in Razorpay before using vendor payouts.'
    : (message || defaultMessage);

  const normalized = createApiError(
    finalStatusCode,
    finalMessage
  );

  normalized.razorpay = {
    statusCode: Number.isFinite(statusCode) ? statusCode : defaultStatusCode,
    error: responseError || err?.error || null,
  };

  return normalized;
};

const getErrorLogMeta = (err) => {
  return {
    name: err?.name,
    message: err?.message,
    statusCode: err?.statusCode,
    code: err?.code,
    // axios-ish
    axios: isAxiosLikeError(err)
      ? {
          status: err?.response?.status,
          data: err?.response?.data,
        }
      : undefined,
    // razorpay-ish
    razorpay: isRazorpayLikeError(err)
      ? {
          statusCode: err?.statusCode,
          error: err?.error,
        }
      : undefined,
  };
};

module.exports = {
  isAxiosLikeError,
  normalizeAxiosError,
  isRazorpayLikeError,
  normalizeRazorpayError,
  getErrorLogMeta,
};
