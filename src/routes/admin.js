const express = require("express");

const { requireAuth } = require("../middleware/auth");
const { getAdminDashboard } = require("../services/admin-service");

const router = express.Router();

router.use(requireAuth);

router.get("/dashboard", async (_req, res, next) => {
  try {
    const result = await getAdminDashboard();
    res.json(result);
  } catch (error) {
    next(error);
  }
});

module.exports = router;
