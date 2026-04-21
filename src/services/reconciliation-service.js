function value(row, camelKey, snakeKey = null) {
  return row[camelKey] ?? row[snakeKey || camelKey];
}

function numberValue(row, camelKey, snakeKey = null) {
  const raw = value(row, camelKey, snakeKey);
  const parsed = Number(raw || 0);
  return Number.isNaN(parsed) ? 0 : parsed;
}

function reconcileCompany({ companyPan, glRows, tasRows, panMetadataByPan, tolerance, onlyFinal }) {
  const tasFiltered = onlyFinal ? tasRows.filter((row) => value(row, "bookingStatus", "booking_status") === "F") : tasRows;

  const glAgg = new Map();
  const glUnmappedAssignments = [];
  for (const row of glRows) {
    if (!row.pan) {
      glUnmappedAssignments.push(row);
      continue;
    }

    const pan = value(row, "pan");
    const monthKey = value(row, "monthKey", "month_key");
    if (!monthKey) {
      continue;
    }

    const key = `${pan}|${monthKey}`;
    if (!glAgg.has(key)) {
      glAgg.set(key, {
        pan,
        monthKey,
        monthLabel: value(row, "monthLabel", "month_label"),
        financialYear: value(row, "financialYear", "financial_year"),
        quarterLabel: value(row, "quarterLabel", "quarter_label"),
        amount: 0,
        count: 0,
        tans: new Set(),
        assignments: new Set(),
      });
    }

    const aggregate = glAgg.get(key);
    aggregate.amount += numberValue(row, "amount");
    aggregate.count += 1;
    if (value(row, "tanBook", "tan_book")) aggregate.tans.add(value(row, "tanBook", "tan_book"));
    if (value(row, "assignment")) aggregate.assignments.add(value(row, "assignment"));
  }

  const tasAgg = new Map();
  const tasUnmappedTans = [];
  for (const row of tasFiltered) {
    const pan = value(row, "pan");
    if (!pan) {
      tasUnmappedTans.push(row);
      continue;
    }

    const monthKey = value(row, "monthKey", "month_key");
    if (!monthKey) {
      continue;
    }

    const key = `${pan}|${monthKey}`;
    if (!tasAgg.has(key)) {
      tasAgg.set(key, {
        pan,
        monthKey,
        monthLabel: value(row, "monthLabel", "month_label"),
        financialYear: value(row, "financialYear", "financial_year"),
        quarterLabel: value(row, "quarterLabel", "quarter_label"),
        tds: 0,
        count: 0,
        tans: new Set(),
        sections: new Set(),
      });
    }

    const aggregate = tasAgg.get(key);
    aggregate.tds += numberValue(row, "taxDeducted", "tax_deducted");
    aggregate.count += 1;
    aggregate.tans.add(value(row, "tan"));
    if (value(row, "sectionCode", "section_code")) aggregate.sections.add(value(row, "sectionCode", "section_code"));
  }

  const allKeys = new Set([...glAgg.keys(), ...tasAgg.keys()]);
  const reconRows = [];

  for (const key of allKeys) {
    const gl = glAgg.get(key);
    const tas = tasAgg.get(key);
    const pan = (gl || tas).pan;
    const diff = Number((((gl ? gl.amount : 0) - (tas ? tas.tds : 0)).toFixed(2)));

    let status = "Amount Mismatch";
    if (!gl) {
      status = "26AS Only";
    } else if (!tas) {
      status = "GL Only";
    } else if (Math.abs(diff) <= tolerance) {
      status = "Perfect Match";
    }

    const metadata = panMetadataByPan.get(pan) || {};
    const glTans = gl ? [...gl.tans].sort() : [];
    const tasTans = tas ? [...tas.tans].sort() : [];
    const intersection = glTans.filter((tan) => tasTans.includes(tan));

    reconRows.push({
      companyPan,
      pan,
      monthKey: (gl || tas).monthKey,
      monthLabel: (gl || tas).monthLabel,
      financialYear: (gl || tas).financialYear,
      quarterLabel: (gl || tas).quarterLabel,
      customerName: metadata.exposureCustomerName || metadata.customerName || "(no master)",
      region: metadata.region || "",
      salesman: metadata.salesman || "",
      rating: metadata.rating || "",
      glAmount: Number((gl ? gl.amount : 0).toFixed(2)),
      tasTds: Number((tas ? tas.tds : 0).toFixed(2)),
      differenceAmount: diff,
      glCount: gl ? gl.count : 0,
      tasCount: tas ? tas.count : 0,
      glTans: glTans.join(", "),
      tasTans: tasTans.join(", "),
      tanCheck: glTans.length > 0 && tasTans.length > 0 ? (intersection.length > 0 ? "Match" : "TAN Mismatch") : "",
      sections: tas ? [...tas.sections].sort().join(", ") : "",
      assignments: gl ? [...gl.assignments].sort().slice(0, 5).join(", ") : "",
      status,
    });
  }

  reconRows.sort((left, right) => Math.abs(right.differenceAmount) - Math.abs(left.differenceAmount));

  const panSummaryMap = new Map();
  for (const row of reconRows) {
    if (!panSummaryMap.has(row.pan)) {
      panSummaryMap.set(row.pan, {
        companyPan: row.companyPan,
        pan: row.pan,
        customerName: row.customerName,
        region: row.region,
        salesman: row.salesman,
        rating: row.rating,
        glAmount: 0,
        tasTds: 0,
        glCount: 0,
        tasCount: 0,
        monthCount: 0,
        glTans: new Set(),
        tasTans: new Set(),
      });
    }

    const summary = panSummaryMap.get(row.pan);
    summary.glAmount += row.glAmount;
    summary.tasTds += row.tasTds;
    summary.glCount += row.glCount;
    summary.tasCount += row.tasCount;
    summary.monthCount += 1;
    String(row.glTans || "").split(",").map((value) => value.trim()).filter(Boolean).forEach((value) => summary.glTans.add(value));
    String(row.tasTans || "").split(",").map((value) => value.trim()).filter(Boolean).forEach((value) => summary.tasTans.add(value));
  }

  const panSummary = [...panSummaryMap.values()].map((summary) => {
    const differenceAmount = Number((summary.glAmount - summary.tasTds).toFixed(2));
    let status = "Transaction more in 26AS";
    if (Math.abs(differenceAmount) <= tolerance) {
      status = "Perfect Match";
    } else if (summary.tasCount === 0) {
      status = "Party not in 26AS";
    } else if (summary.glCount === 0) {
      status = "Party not in Books";
    } else if (differenceAmount > 0) {
      status = "Transaction more in Books";
    }

    return {
      ...summary,
      glAmount: Number(summary.glAmount.toFixed(2)),
      tasTds: Number(summary.tasTds.toFixed(2)),
      differenceAmount,
      glTans: [...summary.glTans].sort().join(", "),
      tasTans: [...summary.tasTans].sort().join(", "),
      status,
    };
  }).sort((left, right) => Math.abs(right.differenceAmount) - Math.abs(left.differenceAmount));

  const rollupBy = (rows, keyFn, labelFn) => {
    const grouped = new Map();
    rows.forEach((row) => {
      const key = keyFn(row);
      if (!grouped.has(key)) {
        grouped.set(key, {
          key,
          label: labelFn(row),
          glAmount: 0,
          tasTds: 0,
          glCount: 0,
          tasCount: 0,
          perfect: 0,
          mismatch: 0,
          glOnly: 0,
          tasOnly: 0,
        });
      }
      const aggregate = grouped.get(key);
      aggregate.glAmount += row.glAmount;
      aggregate.tasTds += row.tasTds;
      aggregate.glCount += row.glCount;
      aggregate.tasCount += row.tasCount;
      if (["Perfect Match", "Perfect Match (M)", "Perfect Match (Q)"].includes(row.status)) aggregate.perfect += 1;
      else if (row.status === "Amount Mismatch") aggregate.mismatch += 1;
      else if (row.status === "GL Only") aggregate.glOnly += 1;
      else if (row.status === "26AS Only") aggregate.tasOnly += 1;
    });
    return [...grouped.values()]
      .map((aggregate) => ({
        ...aggregate,
        diff: Number((aggregate.glAmount - aggregate.tasTds).toFixed(2)),
      }))
      .sort((left, right) => Math.abs(right.diff) - Math.abs(left.diff));
  };

  const fySummaryMap = new Map();
  const panFyMap = new Map();
  reconRows.forEach((row) => {
    if (!fySummaryMap.has(row.financialYear)) {
      fySummaryMap.set(row.financialYear, {
        fy: row.financialYear,
        glAmount: 0,
        tasTds: 0,
        glCount: 0,
        tasCount: 0,
        perfect: 0,
        mismatch: 0,
        glOnly: 0,
        tasOnly: 0,
        panSet: new Set(),
      });
    }
    const fySummary = fySummaryMap.get(row.financialYear);
    fySummary.glAmount += row.glAmount;
    fySummary.tasTds += row.tasTds;
    fySummary.glCount += row.glCount;
    fySummary.tasCount += row.tasCount;
    fySummary.panSet.add(row.pan);
    if (["Perfect Match", "Perfect Match (M)", "Perfect Match (Q)"].includes(row.status)) fySummary.perfect += 1;
    else if (row.status === "Amount Mismatch") fySummary.mismatch += 1;
    else if (row.status === "GL Only") fySummary.glOnly += 1;
    else if (row.status === "26AS Only") fySummary.tasOnly += 1;

    const panFyKey = `${row.pan}|${row.financialYear}`;
    if (!panFyMap.has(panFyKey)) {
      panFyMap.set(panFyKey, {
        pan: row.pan,
        fy: row.financialYear,
        customer: row.customerName,
        region: row.region,
        salesman: row.salesman,
        rating: row.rating,
        glAmount: 0,
        tasTds: 0,
        glCount: 0,
        tasCount: 0,
        monthCount: 0,
      });
    }
    const panFy = panFyMap.get(panFyKey);
    panFy.glAmount += row.glAmount;
    panFy.tasTds += row.tasTds;
    panFy.glCount += row.glCount;
    panFy.tasCount += row.tasCount;
    panFy.monthCount += 1;
  });

  const fySummary = [...fySummaryMap.values()].map((row) => ({
    fy: row.fy,
    glAmount: Number(row.glAmount.toFixed(2)),
    tasTds: Number(row.tasTds.toFixed(2)),
    variance: Number((row.glAmount - row.tasTds).toFixed(2)),
    panCount: row.panSet.size,
    glCount: row.glCount,
    tasCount: row.tasCount,
    perfect: row.perfect,
    mismatch: row.mismatch,
    glOnly: row.glOnly,
    tasOnly: row.tasOnly,
  })).sort((left, right) => left.fy.localeCompare(right.fy));

  const panFy = [...panFyMap.values()].map((row) => {
    const variance = Number((row.glAmount - row.tasTds).toFixed(2));
    let status = "Transaction more in 26AS";
    if (Math.abs(variance) <= tolerance) status = "Perfect Match";
    else if (row.tasCount === 0) status = "Party not in 26AS";
    else if (row.glCount === 0) status = "Party not in Books";
    else if (variance > 0) status = "Transaction more in Books";
    return {
      ...row,
      glAmount: Number(row.glAmount.toFixed(2)),
      tasTds: Number(row.tasTds.toFixed(2)),
      variance,
      status,
    };
  }).sort((left, right) => {
    if (left.fy !== right.fy) return left.fy.localeCompare(right.fy);
    return Math.abs(right.variance) - Math.abs(left.variance);
  });

  const tanMap = new Map();
  glRows.forEach((row) => {
    const tanBook = value(row, "tanBook", "tan_book");
    if (!tanBook) return;
    if (!tanMap.has(tanBook)) {
      tanMap.set(tanBook, {
        key: tanBook,
        label: tanBook,
        glAmount: 0,
        tasTds: 0,
        glCount: 0,
        tasCount: 0,
        perfect: 0,
        mismatch: 0,
        glOnly: 0,
        tasOnly: 0,
        deductor: "",
      });
    }
    const tan = tanMap.get(tanBook);
    tan.glAmount += numberValue(row, "amount");
    tan.glCount += 1;
  });
  tasFiltered.forEach((row) => {
    const rowTan = value(row, "tan");
    if (!tanMap.has(rowTan)) {
      tanMap.set(rowTan, {
        key: rowTan,
        label: rowTan,
        glAmount: 0,
        tasTds: 0,
        glCount: 0,
        tasCount: 0,
        perfect: 0,
        mismatch: 0,
        glOnly: 0,
        tasOnly: 0,
        deductor: value(row, "deductorName", "deductor_name") || "",
      });
    }
    const tan = tanMap.get(rowTan);
    tan.tasTds += numberValue(row, "taxDeducted", "tax_deducted");
    tan.tasCount += 1;
    tan.deductor = value(row, "deductorName", "deductor_name") || tan.deductor;
  });

  const tanSummary = [...tanMap.values()]
    .map((row) => ({
      ...row,
      diff: Number((row.glAmount - row.tasTds).toFixed(2)),
    }))
    .sort((left, right) => Math.abs(right.diff) - Math.abs(left.diff));

  const reports = {
    pan: rollupBy(reconRows, (row) => row.pan, (row) => `${row.pan} - ${row.customerName}`),
    customer: rollupBy(reconRows, (row) => row.customerName || "(blank)", (row) => row.customerName || "(blank)"),
    region: rollupBy(reconRows, (row) => row.region || "(blank)", (row) => row.region || "(blank)"),
    salesman: rollupBy(reconRows, (row) => row.salesman || "(blank)", (row) => row.salesman || "(blank)"),
    rating: rollupBy(reconRows, (row) => row.rating || "(blank)", (row) => row.rating || "(blank)"),
    pansummary: panSummary.map((row) => ({
      ...row,
      variance: row.differenceAmount,
      customer: row.customerName,
      months: reconRows
        .filter((item) => item.pan === row.pan)
        .map((item) => ({
          monthLabel: item.monthLabel,
          monthKey: item.monthKey,
          fy: item.financialYear,
          quarter: item.quarterLabel,
          glAmount: item.glAmount,
          tasTds: item.tasTds,
          diff: item.differenceAmount,
          glCount: item.glCount,
          tasCount: item.tasCount,
          status: item.status,
          sections: item.sections,
          glTans: item.glTans,
          tasTans: item.tasTans,
        }))
        .sort((left, right) => left.monthKey.localeCompare(right.monthKey)),
    })),
    fySummary,
    panFy,
    tan: tanSummary,
  };

  return {
    reconRows,
    panSummary,
    reports,
    exceptions: {
      glUnmappedAssignments: glUnmappedAssignments.map((row) => ({
        assignment: value(row, "assignment"),
        documentNumber: value(row, "documentNumber", "document_number"),
        postingDate: value(row, "postingDate", "posting_date"),
        amount: numberValue(row, "amount"),
        tanBookRaw: value(row, "tanBookRaw", "tan_book_raw"),
        issue: "Assignment not found in Agreement to PAN master",
      })),
      tasUnmappedTans: [...new Set(tasUnmappedTans.map((row) => value(row, "tan")))].map((tan) => {
        const rows = tasUnmappedTans.filter((item) => value(item, "tan") === tan);
        return {
          tan,
          deductorName: value(rows[0] || {}, "deductorName", "deductor_name") || "",
          txnCount: rows.length,
          totalTds: Number(rows.reduce((sum, item) => sum + numberValue(item, "taxDeducted", "tax_deducted"), 0).toFixed(2)),
          issue: "TAN not found in TAN to PAN master",
        };
      }),
      tanCrossCheckMismatches: reconRows.filter((row) => row.tanCheck === "TAN Mismatch"),
      nonFinalTransactions: tasRows.filter((row) => value(row, "bookingStatus", "booking_status") !== "F"),
    },
  };
}

module.exports = {
  reconcileCompany,
};
