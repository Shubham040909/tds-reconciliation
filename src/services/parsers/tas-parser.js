const fs = require("fs");

const { TAN_RE, fyFromDate, monthKey, monthLabel, parseDdMonYyyy, quarterFromDate } = require("../../utils/recon-helpers");

function parse26AsFile(filePath, fileName, masterLookup) {
  const text = fs.readFileSync(filePath, "utf8");
  const caretCount = (text.match(/\^/g) || []).length;
  const tabCount = (text.match(/\t/g) || []).length;
  const delimiter = caretCount > tabCount ? "^" : "\t";

  const lines = text.replace(/\r/g, "").split("\n");

  let startIndex = -1;
  let endIndex = lines.length;

  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index];
    if (
      (line.includes("Tax Deducted at Source") || line.includes("TAX DEDUCTED AT SOURCE"))
      && (/PART[\s-]*I\b/i.test(line) || /PART[\s-]*A\b/i.test(line))
      && !/PART[\s-]*I{2,}/i.test(line)
      && !/PART[\s-]*A[12]/i.test(line)
    ) {
      startIndex = index;
      break;
    }
  }

  for (let index = startIndex + 1; index < lines.length; index += 1) {
    const line = lines[index];
    if ((/PART[\s-]*(II|A[12]|B|C)\b/i.test(line) && /details|tax/i.test(line))
      || line.includes("PART-II")
      || line.includes("PART A1")
      || line.includes("PART A2")
      || line.includes("PART B")
      || line.includes("PART C")) {
      endIndex = index;
      break;
    }
  }

  let filePan = null;
  const fileMatch = fileName.match(/\b([A-Z]{5}\d{4}[A-Z])\b/i);
  if (fileMatch) {
    filePan = fileMatch[1].toUpperCase();
  }

  if (!filePan) {
    for (let index = 0; index < Math.min(15, lines.length); index += 1) {
      if (/permanent account number/i.test(lines[index]) && lines[index + 1]) {
        const cells = lines[index + 1].split(delimiter).map((value) => value.trim());
        const panCell = cells.find((value) => /^[A-Z]{5}\d{4}[A-Z]$/.test(value));
        if (panCell) {
          filePan = panCell;
          break;
        }
      }
    }
  }

  const transactions = [];
  const deductors = [];
  let currentTan = null;
  let currentDeductor = null;

  for (let index = startIndex; index < endIndex; index += 1) {
    const line = lines[index];
    if (!line.trim()) {
      continue;
    }

    const fields = line.split(delimiter).map((value) => value.trim());
    if (fields.length === 0) {
      continue;
    }

    if (/^\d+$/.test(fields[0]) && fields.length >= 3) {
      let tanIndex = -1;
      for (let offset = 2; offset < Math.min(fields.length, 5); offset += 1) {
        if (TAN_RE.test(fields[offset])) {
          tanIndex = offset;
          break;
        }
      }

      if (tanIndex > 0) {
        currentTan = fields[tanIndex];
        currentDeductor = fields[1] || "";
        deductors.push({
          tan: currentTan,
          name: currentDeductor,
        });
        continue;
      }
    }

    if (fields[0] === "" && fields.length >= 10 && currentTan) {
      if (!/^\d+$/.test(fields[1])) {
        continue;
      }

      const transactionDate = parseDdMonYyyy(fields[3]);
      if (!transactionDate) {
        continue;
      }

      const bookingDate = parseDdMonYyyy(fields[5]);
      const amountPaid = Number.parseFloat(String(fields[7] || "0").replace(/,/g, ""));
      const taxDeducted = Number.parseFloat(String(fields[8] || "0").replace(/,/g, ""));
      const tdsDeposited = Number.parseFloat(String(fields[9] || "0").replace(/,/g, ""));

      if (Number.isNaN(amountPaid) || Number.isNaN(taxDeducted)) {
        continue;
      }

      const mk = monthKey(transactionDate);
      transactions.push({
        companyPan: filePan,
        tan: currentTan,
        deductorName: currentDeductor,
        sectionCode: fields[2] || "",
        transactionDate,
        bookingStatus: fields[4] || "",
        bookingDate,
        remarks: fields[6] || "",
        amountPaid,
        taxDeducted,
        tdsDeposited: Number.isNaN(tdsDeposited) ? 0 : tdsDeposited,
        pan: masterLookup.tanToPan.get(currentTan) || null,
        monthKey: mk,
        monthLabel: monthLabel(mk),
        financialYear: fyFromDate(transactionDate),
        quarterLabel: quarterFromDate(transactionDate),
      });
    }
  }

  return {
    filePan,
    deductors,
    transactions,
  };
}

module.exports = {
  parse26AsFile,
};
