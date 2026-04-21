const { getUserFromToken } = require("../services/auth-service");

async function requireAuth(req, res, next) {
  try {
    const header = req.get("authorization") || "";
    const token = header.startsWith("Bearer ") ? header.slice(7) : "";
    const user = await getUserFromToken(token);
    if (!user) {
      res.status(401).json({ error: "Login required." });
      return;
    }
    req.user = user;
    next();
  } catch (error) {
    next(error);
  }
}

module.exports = {
  requireAuth,
};
