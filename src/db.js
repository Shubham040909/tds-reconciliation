// ✅ Load env FIRST (very important)
require("dotenv").config({ path: "../.env" });

const { Pool } = require("pg");

// ✅ Validate ENV early (fail fast)
if (!process.env.DATABASE_URL) {
  console.error("❌ DATABASE_URL is missing in .env");
  process.exit(1);
}

// ✅ Debug (optional)
console.log("DB URL:", process.env.DATABASE_URL ? "Loaded ✅" : "Missing ❌");

// ✅ Decide SSL usage
function shouldUseSsl() {
  if (process.env.DATABASE_SSL) {
    return process.env.DATABASE_SSL.toLowerCase() === "true";
  }

  return /supabase\.(co|com)|pooler\.supabase\.com/i.test(
    process.env.DATABASE_URL || ""
  );
}

// ✅ Create pool
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: shouldUseSsl()
    ? { rejectUnauthorized: false }
    : false,
  max: 10, // optional: connection pool size
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
});

// ✅ Query helper
async function query(text, params) {
  try {
    return await pool.query(text, params);
  } catch (err) {
    console.error("❌ DB Query Error:", err.message);
    throw err;
  }
}

// ✅ Transaction helper
async function withTransaction(fn) {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const result = await fn(client);

    await client.query("COMMIT");
    return result;
  } catch (error) {
    await client.query("ROLLBACK");
    console.error("❌ Transaction Error:", error.message);
    throw error;
  } finally {
    client.release();
  }
}

// ✅ Test DB connection (run once)
async function testConnection() {
  try {
    const res = await pool.query("SELECT NOW()");
    console.log("✅ DB Connected:", res.rows[0].now);
  } catch (err) {
    console.error("❌ DB Connection Failed:", err.message);
  }
}

// Run once (can remove later)
testConnection();

module.exports = {
  pool,
  query,
  withTransaction,
};