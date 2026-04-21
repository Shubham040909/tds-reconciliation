const XLSX = require("xlsx");

const { PAN_RE, TAN_RE, safeUpper } = require("../../utils/recon-helpers");
const { badRequest } = require("../../utils/errors");

function findSheetName(sheetNames, search) {
  const lower = search.toLowerCase();
  return sheetNames.find((name) => name.toLowerCase() === lower)
    || sheetNames.find((name) => name.toLowerCase().includes(lower));
}

function parseMasterWorkbook(filePath) {
  const workbook = XLSX.readFile(filePath, {
    cellDates: true,
  });

  const sheetNames = workbook.SheetNames || [];
  const agreementSheet = findSheetName(sheetNames, "Agreement to PAN") || findSheetName(sheetNames, "Agreement");
  const tanSheet = findSheetName(sheetNames, "TAN to PAN") || findSheetName(sheetNames, "TAN");
  const panSheet = findSheetName(sheetNames, "PAN to other") || findSheetName(sheetNames, "PAN");

  if (!agreementSheet || !tanSheet || !panSheet) {
    throw badRequest("Master file must contain Agreement to PAN, TAN to PAN, and PAN to other sheets.");
  }

  const agreementRows = XLSX.utils.sheet_to_json(workbook.Sheets[agreementSheet], { header: 1, defval: null }).slice(1);
  const tanRows = XLSX.utils.sheet_to_json(workbook.Sheets[tanSheet], { header: 1, defval: null }).slice(1);
  const panRows = XLSX.utils.sheet_to_json(workbook.Sheets[panSheet], { header: 1, defval: null }).slice(1);

  const agreements = [];
  const tans = [];
  const panMetadata = [];
  const issues = {
    duplicateAgreements: [],
    duplicateTans: [],
    agreementIssues: [],
    tanIssues: [],
  };

  const seenAgreements = new Set();
  const seenTans = new Set();

  for (const row of agreementRows) {
    if (!row[0]) {
      continue;
    }

    const agreementCode = safeUpper(row[0]);
    const pan = row[1] ? safeUpper(row[1]) : null;

    if (seenAgreements.has(agreementCode)) {
      issues.duplicateAgreements.push(agreementCode);
    }
    if (pan && !PAN_RE.test(pan)) {
      issues.agreementIssues.push({
        agreementCode,
        pan,
        issue: "Invalid PAN format",
      });
    }

    seenAgreements.add(agreementCode);
    agreements.push({ agreementCode, pan });
  }

  for (const row of tanRows) {
    if (!row[0]) {
      continue;
    }

    const tan = safeUpper(row[0]);
    const pan = row[1] ? safeUpper(row[1]) : null;

    if (seenTans.has(tan)) {
      issues.duplicateTans.push(tan);
    }
    if (!TAN_RE.test(tan)) {
      issues.tanIssues.push({
        tan,
        pan,
        issue: "Invalid TAN format",
      });
    }
    if (pan && !PAN_RE.test(pan)) {
      issues.tanIssues.push({
        tan,
        pan,
        issue: "Invalid PAN format",
      });
    }

    seenTans.add(tan);
    tans.push({ tan, pan });
  }

  for (const row of panRows) {
    if (!row[0]) {
      continue;
    }

    panMetadata.push({
      pan: safeUpper(row[0]),
      customerName: row[1] ? String(row[1]).trim() : "",
      region: row[2] ? String(row[2]).trim() : "",
      salesman: row[3] ? String(row[3]).trim() : "",
      exposureCustomerName: row[4] ? String(row[4]).trim() : (row[1] ? String(row[1]).trim() : ""),
      rating: row[5] === null || row[5] === undefined ? "" : String(row[5]).trim(),
    });
  }

  return {
    agreements,
    tans,
    panMetadata,
    issues,
  };
}

module.exports = {
  parseMasterWorkbook,
};
