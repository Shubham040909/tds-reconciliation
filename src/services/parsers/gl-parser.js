const XLSX = require("xlsx");

const { badRequest } = require("../../utils/errors");
const {
  TAN_RE,
  excelDate,
  fyFromDate,
  monthKey,
  monthLabel,
  quarterFromDate,
  safeUpper,
} = require("../../utils/recon-helpers");

function findHeaderRow(rows) {
  const expectedHeaderTokens = ["account", "assignment", "posting", "amount", "document", "company", "tan"];
  const sampleSize = Math.min(50, rows.length);

  for (let index = 0; index < sampleSize; index += 1) {
    const row = rows[index] || [];
    const tokens = row.map((cell) => String(cell || "").trim().toLowerCase());
    const hits = expectedHeaderTokens.filter((token) => tokens.some((cell) => cell.includes(token))).length;
    if (hits >= 3) {
      return index;
    }
  }

  return -1;
}

function findColumnIndex(headers, ...names) {
  for (const name of names) {
    const index = headers.findIndex((header) => header === name.toLowerCase());
    if (index >= 0) {
      return index;
    }
  }

  for (const name of names) {
    const index = headers.findIndex((header) => header.includes(name.toLowerCase()));
    if (index >= 0) {
      return index;
    }
  }

  return -1;
}

function parseGlWorkbook(filePath, masterLookup) {
  const workbook = XLSX.readFile(filePath, {
    cellDates: true,
  });

  if (!workbook.SheetNames || workbook.SheetNames.length === 0) {
    throw badRequest("GL workbook does not contain any sheets.");
  }

  let rows = null;
  let sheetName = null;
  for (const candidate of workbook.SheetNames) {
    const candidateRows = XLSX.utils.sheet_to_json(workbook.Sheets[candidate], {
      header: 1,
      defval: null,
      blankrows: false,
    });
    if (candidateRows.length > 0) {
      rows = candidateRows;
      sheetName = candidate;
      break;
    }
  }

  if (!rows || rows.length === 0) {
    throw badRequest("Unable to read rows from the GL workbook.");
  }

  const headerRowIndex = findHeaderRow(rows);
  if (headerRowIndex < 0) {
    throw badRequest("Could not detect the GL header row.");
  }

  const headers = rows[headerRowIndex].map((value) => String(value || "").trim().toLowerCase());
  const cAccount = findColumnIndex(headers, "account");
  const cAssignment = findColumnIndex(headers, "assignment");
  const cDoc = findColumnIndex(headers, "document number", "doc no", "document no");
  const cCompanyCode = findColumnIndex(headers, "company code", "co code", "company");
  const cText = findColumnIndex(headers, "text", "narration");
  const cReference = findColumnIndex(headers, "reference", "voucher");
  const cPostingDate = findColumnIndex(headers, "posting date", "post date");
  const cDocumentDate = findColumnIndex(headers, "document date", "doc date");
  const cAmount = findColumnIndex(headers, "amount in local currency", "amount", "amt");
  const cTan = findColumnIndex(headers, "tan no", "tan");
  const cCurrency = findColumnIndex(headers, "local currency", "currency");

  const missing = [];
  if (cAssignment < 0) missing.push("Assignment");
  if (cCompanyCode < 0) missing.push("Company Code");
  if (cPostingDate < 0) missing.push("Posting Date");
  if (cAmount < 0) missing.push("Amount");
  if (missing.length > 0) {
    throw badRequest(`GL workbook is missing required columns: ${missing.join(", ")}`);
  }

  const entries = [];
  for (let index = headerRowIndex + 1; index < rows.length; index += 1) {
    const row = rows[index];
    if (!row) {
      continue;
    }

    if (cAccount >= 0 && (row[cAccount] === null || row[cAccount] === undefined || row[cAccount] === "")) {
      continue;
    }

    const postingDate = excelDate(row[cPostingDate]);
    if (!postingDate) {
      continue;
    }

    const amountValue = row[cAmount];
    const amount = typeof amountValue === "number"
      ? amountValue
      : Number.parseFloat(String(amountValue || "").replace(/,/g, ""));

    if (Number.isNaN(amount)) {
      continue;
    }

    const assignment = safeUpper(row[cAssignment]);
    const pan = masterLookup.agreementToPan.get(assignment) || null;
    const tanBookRaw = row[cTan] ? String(row[cTan]).trim() : "";
    const tanBook = TAN_RE.test(safeUpper(tanBookRaw)) ? safeUpper(tanBookRaw) : "";
    const mk = monthKey(postingDate);

    entries.push({
      sheetName,
      account: cAccount >= 0 ? String(row[cAccount] || "").trim() : "",
      assignment,
      documentNumber: cDoc >= 0 && row[cDoc] !== null && row[cDoc] !== undefined ? String(row[cDoc]).trim() : "",
      companyCode: cCompanyCode >= 0 ? String(row[cCompanyCode] || "").trim() : "",
      postingDate,
      documentDate: cDocumentDate >= 0 ? excelDate(row[cDocumentDate]) : null,
      amount,
      localCurrency: cCurrency >= 0 ? String(row[cCurrency] || "").trim() : "",
      textValue: cText >= 0 ? String(row[cText] || "").trim() : "",
      reference: cReference >= 0 ? String(row[cReference] || "").trim() : "",
      tanBook,
      tanBookRaw,
      pan,
      monthKey: mk,
      monthLabel: monthLabel(mk),
      financialYear: fyFromDate(postingDate),
      quarterLabel: quarterFromDate(postingDate),
    });
  }

  return {
    sheetName,
    entries,
  };
}

module.exports = {
  parseGlWorkbook,
};
