const extractUser = (req, res, next) => {
  const authId = req.headers['x-auth-id'];
  const userId = req.headers['x-user-id'];
  const email = req.headers['x-user-email'];
  const username = req.headers['x-user-username'];
  const role = req.headers['x-user-role'];

  if (authId || userId) {
    req.user = {
      authId,
      userId,
      email,
      username,
      role,
    };
  }

  next();
};

module.exports = { extractUser };
