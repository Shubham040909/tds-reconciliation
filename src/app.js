const cors = require("cors");
const express = require("express");

const { query } = require("./db");
const adminRoutes = require("./routes/admin");
const authRoutes = require("./routes/auth");
const projectRoutes = require("./routes/projects");

function createApp() {
  const app = express();

  app.use(cors({
    origin: true,
    credentials: false,
  }));
  app.use(express.json({ limit: "10mb" }));
  app.use(express.urlencoded({ extended: true }));

  app.get("/health", (_req, res) => {
    res.json({ ok: true });
  });

  app.get("/health/db", async (_req, res, next) => {
    try {
      const result = await query("select current_database() as database, current_schema() as schema, now() as server_time");
      res.json({ ok: true, ...result.rows[0] });
    } catch (error) {
      next(error);
    }
  });

  app.use("/api/auth", authRoutes);
  app.use("/api/admin", adminRoutes);
  app.use("/api/projects", projectRoutes);

  app.use((err, _req, res, _next) => {
    console.error(err);
    res.status(err.statusCode || 500).json({
      error: err.message || "Internal server error",
    });
  });

  return app;
}

module.exports = { createApp };
