const createApiError = (statusCode, message, isOperational = true) => {
  const error = new Error(message);
  error.statusCode = statusCode;
  error.isOperational = isOperational;
  error.timestamp = new Date().toISOString();
  Error.captureStackTrace(error, createApiError);
  return error;
};

module.exports = createApiError;
