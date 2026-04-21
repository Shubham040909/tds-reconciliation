require("dotenv").config();

const { pool } = require("../db");
const { createUser, ensureAuthSchema } = require("../services/auth-service");

async function main() {
  const name = process.env.ADMIN_USERNAME || "admin";
  const password = process.env.ADMIN_PASSWORD || "admin123";
  await ensureAuthSchema();
  const user = await createUser({ name, password, role: "admin" });
  console.log(`Admin user ready: ${user.name}`);
}

main()
  .catch((error) => {
    console.error(error.message);
    process.exitCode = 1;
  })
  .finally(async () => {
    await pool.end();
  });
