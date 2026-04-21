const express = require("express");

const { createUser, getUserFromToken, login } = require("../services/auth-service");
const { requireAuth } = require("../middleware/auth");

const router = express.Router();

router.post("/login", async (req, res, next) => {
  try {
    const result = await login(req.body);
    res.json(result);
  } catch (error) {
    next(error);
  }
});

router.get("/me", requireAuth, async (req, res) => {
  res.json({ user: req.user });
});

router.post("/users", requireAuth, async (req, res, next) => {
  try {
    if (req.user.role !== "admin") {
      res.status(403).json({ error: "Admin access required." });
      return;
    }
    const user = await createUser(req.body);
    res.status(201).json({ user });
  } catch (error) {
    next(error);
  }
});

module.exports = router;
