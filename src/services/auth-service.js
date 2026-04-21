const crypto = require("crypto");
const { v4: uuid } = require("uuid");

const { query } = require("../db");
const { badRequest } = require("../utils/errors");

const TOKEN_SECRET = process.env.AUTH_TOKEN_SECRET || process.env.DATABASE_URL || "local-dev-secret";
const TOKEN_TTL_MS = 12 * 60 * 60 * 1000;

function base64url(input) {
  return Buffer.from(input).toString("base64url");
}

function signPayload(payload) {
  const encoded = base64url(JSON.stringify(payload));
  const signature = crypto.createHmac("sha256", TOKEN_SECRET).update(encoded).digest("base64url");
  return `${encoded}.${signature}`;
}

function verifyToken(token) {
  if (!token || !token.includes(".")) {
    return null;
  }

  const [encoded, signature] = token.split(".");
  const expected = crypto.createHmac("sha256", TOKEN_SECRET).update(encoded).digest("base64url");
  if (Buffer.byteLength(signature) !== Buffer.byteLength(expected)) {
    return null;
  }
  if (!crypto.timingSafeEqual(Buffer.from(signature), Buffer.from(expected))) {
    return null;
  }

  const payload = JSON.parse(Buffer.from(encoded, "base64url").toString("utf8"));
  if (!payload.exp || Date.now() > payload.exp) {
    return null;
  }
  return payload;
}

function hashPassword(password, salt = crypto.randomBytes(16).toString("hex")) {
  const hash = crypto.scryptSync(password, salt, 64).toString("hex");
  return `scrypt:${salt}:${hash}`;
}

function verifyPassword(password, storedHash) {
  const [scheme, salt, hash] = String(storedHash || "").split(":");
  if (scheme !== "scrypt" || !salt || !hash) {
    return false;
  }

  const candidate = hashPassword(password, salt).split(":")[2];
  return crypto.timingSafeEqual(Buffer.from(candidate, "hex"), Buffer.from(hash, "hex"));
}

async function ensureAuthSchema() {
  await query(`
    create table if not exists app_users (
      id uuid primary key,
      name text not null unique,
      password_hash text not null,
      role text not null default 'admin',
      is_active boolean not null default true,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now()
    )
  `);
}

async function createUser({ name, password, role = "admin" }) {
  const normalizedName = String(name || "").trim();
  if (!normalizedName || !password) {
    throw badRequest("Name and password are required.");
  }

  await ensureAuthSchema();
  const userId = uuid();
  const passwordHash = hashPassword(password);
  await query(
    `insert into app_users (id, name, password_hash, role)
     values ($1, $2, $3, $4)
     on conflict (name)
     do update set password_hash = excluded.password_hash, role = excluded.role, is_active = true, updated_at = now()`,
    [userId, normalizedName, passwordHash, role],
  );

  return {
    name: normalizedName,
    role,
  };
}

async function login({ name, password }) {
  const normalizedName = String(name || "").trim();
  if (!normalizedName || !password) {
    throw badRequest("Name and password are required.");
  }

  await ensureAuthSchema();
  const result = await query(
    "select id, name, password_hash, role, is_active from app_users where name = $1",
    [normalizedName],
  );
  const user = result.rows[0];
  if (!user || !user.is_active || !verifyPassword(password, user.password_hash)) {
    throw badRequest("Invalid name or password.");
  }

  const publicUser = {
    id: user.id,
    name: user.name,
    role: user.role,
  };

  return {
    token: signPayload({
      sub: user.id,
      name: user.name,
      role: user.role,
      exp: Date.now() + TOKEN_TTL_MS,
    }),
    user: publicUser,
  };
}

async function getUserFromToken(token) {
  const payload = verifyToken(token);
  if (!payload) {
    return null;
  }

  await ensureAuthSchema();
  const result = await query(
    "select id, name, role, is_active from app_users where id = $1",
    [payload.sub],
  );
  const user = result.rows[0];
  if (!user || !user.is_active) {
    return null;
  }
  return {
    id: user.id,
    name: user.name,
    role: user.role,
  };
}

module.exports = {
  ensureAuthSchema,
  createUser,
  login,
  getUserFromToken,
};
