const PAN_RE = /^[A-Z]{5}\d{4}[A-Z]$/;
const TAN_RE = /^[A-Z]{4}\d{5}[A-Z]$/;

function safeUpper(value) {
  return String(value || "").trim().toUpperCase();
}

function excelDate(value) {
  if (!value) {
    return null;
  }

  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return value;
  }

  if (typeof value === "number") {
    const utcDays = Math.floor(value - 25569);
    const utcValue = utcDays * 86400;
    return new Date(utcValue * 1000);
  }

  const parsed = new Date(value);
  if (!Number.isNaN(parsed.getTime())) {
    return parsed;
  }

  return null;
}

function monthKey(date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  return `${year}-${month}`;
}

function monthLabel(key) {
  const [year, month] = key.split("-");
  const date = new Date(Number(year), Number(month) - 1, 1);
  return date.toLocaleString("en-IN", {
    month: "short",
    year: "numeric",
  });
}

function fyFromDate(date) {
  const year = date.getFullYear();
  const month = date.getMonth() + 1;
  if (month >= 4) {
    return `${year}-${String(year + 1).slice(-2)}`;
  }
  return `${year - 1}-${String(year).slice(-2)}`;
}

function quarterFromDate(date) {
  const month = date.getMonth() + 1;
  const year = date.getFullYear();
  if (month >= 4 && month <= 6) {
    return `Q1 ${year}-${String(year + 1).slice(-2)}`;
  }
  if (month >= 7 && month <= 9) {
    return `Q2 ${year}-${String(year + 1).slice(-2)}`;
  }
  if (month >= 10 && month <= 12) {
    return `Q3 ${year}-${String(year + 1).slice(-2)}`;
  }
  return `Q4 ${year - 1}-${String(year).slice(-2)}`;
}

function parseDdMonYyyy(value) {
  if (!value) {
    return null;
  }

  const match = String(value).trim().match(/^(\d{1,2})-([A-Za-z]{3})-(\d{2,4})$/);
  if (!match) {
    return null;
  }

  const monthMap = {
    jan: 0,
    feb: 1,
    mar: 2,
    apr: 3,
    may: 4,
    jun: 5,
    jul: 6,
    aug: 7,
    sep: 8,
    oct: 9,
    nov: 10,
    dec: 11,
  };

  const day = Number(match[1]);
  const month = monthMap[match[2].toLowerCase()];
  let year = Number(match[3]);
  if (year < 100) {
    year += 2000;
  }

  return new Date(year, month, day);
}

module.exports = {
  PAN_RE,
  TAN_RE,
  safeUpper,
  excelDate,
  monthKey,
  monthLabel,
  fyFromDate,
  quarterFromDate,
  parseDdMonYyyy,
};
