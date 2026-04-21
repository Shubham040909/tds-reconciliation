const fs = require("fs");
const path = require("path");

function ensureUploadDir() {
  const uploadDir = path.resolve(process.env.UPLOAD_DIR || "uploads");
  if (!fs.existsSync(uploadDir)) {
    fs.mkdirSync(uploadDir, { recursive: true });
  }
  return uploadDir;
}

module.exports = {
  ensureUploadDir,
};
