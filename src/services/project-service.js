const fs = require("fs");
const { v4: uuid } = require("uuid");

const { query, withTransaction } = require("../db");
const { badRequest, notFound } = require("../utils/errors");
const { parseMasterWorkbook } = require("./parsers/master-parser");
const { parseGlWorkbook } = require("./parsers/gl-parser");
const { parse26AsFile } = require("./parsers/tas-parser");
const { reconcileCompany } = require("./reconciliation-service");

async function ensureProject(projectId) {
  const result = await query("select * from projects where id = $1", [projectId]);
  if (result.rowCount === 0) {
    throw notFound(`Project ${projectId} not found.`);
  }
  return result.rows[0];
}

async function createImportBatch(client, projectId, importType, file) {
  const importBatchId = uuid();
  await client.query(
    `insert into import_batches (id, project_id, import_type, file_name, file_size_bytes)
     values ($1, $2, $3, $4, $5)`,
    [importBatchId, projectId, importType, file.originalname, file.size],
  );
  return importBatchId;
}

function dedupeByLast(rows, keyFn) {
  const map = new Map();
  rows.forEach((row) => {
    map.set(keyFn(row), row);
  });
  return [...map.values()];
}

async function bulkInsert(client, tableName, columns, rows, chunkSize = 500) {
  if (!rows.length) {
    return;
  }

  for (let start = 0; start < rows.length; start += chunkSize) {
    const chunk = rows.slice(start, start + chunkSize);
    const values = [];
    const placeholders = chunk.map((row, rowIndex) => {
      const cols = columns.map((column, colIndex) => {
        values.push(row[column]);
        return `$${rowIndex * columns.length + colIndex + 1}`;
      });
      return `(${cols.join(", ")})`;
    });

    await client.query(
      `insert into ${tableName} (${columns.join(", ")}) values ${placeholders.join(", ")}`,
      values,
    );
  }
}

async function loadMasterLookup(projectId) {
  const [agreementRows, tanRows, panRows] = await Promise.all([
    query("select agreement_code, pan from master_agreement_pan where project_id = $1", [projectId]),
    query("select tan, pan from master_tan_pan where project_id = $1", [projectId]),
    query(
      `select pan, customer_name, region, salesman, exposure_customer_name, rating
       from master_pan_metadata
       where project_id = $1`,
      [projectId],
    ),
  ]);

  return {
    agreementToPan: new Map(agreementRows.rows.map((row) => [row.agreement_code, row.pan])),
    tanToPan: new Map(tanRows.rows.map((row) => [row.tan, row.pan])),
    panMetadataByPan: new Map(
      panRows.rows.map((row) => [row.pan, {
        customerName: row.customer_name,
        region: row.region,
        salesman: row.salesman,
        exposureCustomerName: row.exposure_customer_name,
        rating: row.rating,
      }]),
    ),
  };
}

async function createProject(payload) {
  const name = String(payload.name || "").trim();
  if (!name) {
    throw badRequest("Project name is required.");
  }

  const projectId = uuid();
  const clientName = payload.clientName ? String(payload.clientName).trim() : null;
  const financialYear = payload.financialYear ? String(payload.financialYear).trim() : null;
  const tolerance = payload.tolerance === undefined ? 10 : Number(payload.tolerance);
  const onlyFinal = payload.onlyFinal === undefined ? true : Boolean(payload.onlyFinal);

  await query(
    `insert into projects (id, name, client_name, financial_year, tolerance, only_final)
     values ($1, $2, $3, $4, $5, $6)`,
    [projectId, name, clientName, financialYear, tolerance, onlyFinal],
  );

  return {
    id: projectId,
    name,
    clientName,
    financialYear,
    tolerance,
    onlyFinal,
  };
}

async function upsertCompanyCodeMappings(projectId, payload) {
  await ensureProject(projectId);

  const mappings = Array.isArray(payload.mappings) ? payload.mappings : [];
  if (mappings.length === 0) {
    throw badRequest("At least one company code mapping is required.");
  }

  await withTransaction(async (client) => {
    await client.query("delete from company_code_mappings where project_id = $1", [projectId]);

    for (const mapping of mappings) {
      const companyCode = String(mapping.companyCode || "").trim().toUpperCase();
      const companyPan = String(mapping.companyPan || "").trim().toUpperCase();
      if (!companyCode || !companyPan) {
        throw badRequest("Each mapping must include companyCode and companyPan.");
      }

      await client.query(
        `insert into company_code_mappings (id, project_id, company_code, company_pan)
         values ($1, $2, $3, $4)`,
        [uuid(), projectId, companyCode, companyPan],
      );
    }
  });

  return {
    projectId,
    count: mappings.length,
  };
}

async function importMasterWorkbook(projectId, file) {
  if (!file) {
    throw badRequest("Master workbook file is required.");
  }

  await ensureProject(projectId);
  const parsed = parseMasterWorkbook(file.path);

  const result = await withTransaction(async (client) => {
    const importBatchId = await createImportBatch(client, projectId, "master", file);
    const agreements = dedupeByLast(parsed.agreements, (row) => row.agreementCode);
    const tans = dedupeByLast(parsed.tans, (row) => row.tan);
    const panMetadata = dedupeByLast(parsed.panMetadata, (row) => row.pan);

    await client.query("delete from master_agreement_pan where project_id = $1", [projectId]);
    await client.query("delete from master_tan_pan where project_id = $1", [projectId]);
    await client.query("delete from master_pan_metadata where project_id = $1", [projectId]);

    await bulkInsert(
      client,
      "master_agreement_pan",
      ["id", "project_id", "agreement_code", "pan", "import_batch_id"],
      agreements.map((row) => ({
        id: uuid(),
        project_id: projectId,
        agreement_code: row.agreementCode,
        pan: row.pan,
        import_batch_id: importBatchId,
      })),
    );

    await bulkInsert(
      client,
      "master_tan_pan",
      ["id", "project_id", "tan", "pan", "import_batch_id"],
      tans.map((row) => ({
        id: uuid(),
        project_id: projectId,
        tan: row.tan,
        pan: row.pan,
        import_batch_id: importBatchId,
      })),
    );

    await bulkInsert(
      client,
      "master_pan_metadata",
      ["id", "project_id", "pan", "customer_name", "region", "salesman", "exposure_customer_name", "rating", "import_batch_id"],
      panMetadata.map((row) => ({
        id: uuid(),
        project_id: projectId,
        pan: row.pan,
        customer_name: row.customerName,
        region: row.region,
        salesman: row.salesman,
        exposure_customer_name: row.exposureCustomerName,
        rating: row.rating,
        import_batch_id: importBatchId,
      })),
    );

    await client.query(
      "update import_batches set row_count = $2, metadata = $3 where id = $1",
      [
        importBatchId,
        agreements.length + tans.length + panMetadata.length,
        {
          ...parsed.issues,
          originalCounts: {
            agreements: parsed.agreements.length,
            tans: parsed.tans.length,
            panMetadata: parsed.panMetadata.length,
          },
          insertedCounts: {
            agreements: agreements.length,
            tans: tans.length,
            panMetadata: panMetadata.length,
          },
        },
      ],
    );

    return importBatchId;
  });

  return {
    projectId,
    importBatchId: result,
    agreements: dedupeByLast(parsed.agreements, (row) => row.agreementCode).length,
    tans: dedupeByLast(parsed.tans, (row) => row.tan).length,
    panMetadata: dedupeByLast(parsed.panMetadata, (row) => row.pan).length,
    issues: parsed.issues,
  };
}

async function importGlWorkbook(projectId, file) {
  if (!file) {
    throw badRequest("GL workbook file is required.");
  }

  await ensureProject(projectId);
  const masterLookup = await loadMasterLookup(projectId);
  const parsed = parseGlWorkbook(file.path, masterLookup);

  const importBatchId = await withTransaction(async (client) => {
    const batchId = await createImportBatch(client, projectId, "gl", file);

    await bulkInsert(
      client,
      "gl_entries",
      [
        "id", "project_id", "import_batch_id", "source_file_name", "account", "assignment", "document_number",
        "company_code", "posting_date", "document_date", "amount", "local_currency", "text_value", "reference",
        "tan_book", "tan_book_raw", "pan", "month_key", "month_label", "financial_year", "quarter_label",
      ],
      parsed.entries.map((entry) => ({
        id: uuid(),
        project_id: projectId,
        import_batch_id: batchId,
        source_file_name: file.originalname,
        account: entry.account,
        assignment: entry.assignment,
        document_number: entry.documentNumber,
        company_code: entry.companyCode,
        posting_date: entry.postingDate,
        document_date: entry.documentDate,
        amount: entry.amount,
        local_currency: entry.localCurrency,
        text_value: entry.textValue,
        reference: entry.reference,
        tan_book: entry.tanBook,
        tan_book_raw: entry.tanBookRaw,
        pan: entry.pan,
        month_key: entry.monthKey,
        month_label: entry.monthLabel,
        financial_year: entry.financialYear,
        quarter_label: entry.quarterLabel,
      })),
    );

    await client.query(
      "update import_batches set row_count = $2, metadata = $3 where id = $1",
      [batchId, parsed.entries.length, { sheetName: parsed.sheetName }],
    );

    return batchId;
  });

  return {
    projectId,
    importBatchId,
    rowsImported: parsed.entries.length,
    sheetName: parsed.sheetName,
  };
}

async function import26AsText(projectId, file) {
  if (!file) {
    throw badRequest("26AS text file is required.");
  }

  await ensureProject(projectId);
  const masterLookup = await loadMasterLookup(projectId);
  const parsed = parse26AsFile(file.path, file.originalname, masterLookup);

  const importBatchId = await withTransaction(async (client) => {
    const batchId = await createImportBatch(client, projectId, "26as", file);

    await bulkInsert(
      client,
      "tas_transactions",
      [
        "id", "project_id", "import_batch_id", "source_file_name", "company_pan", "tan", "deductor_name",
        "section_code", "transaction_date", "booking_status", "booking_date", "remarks", "amount_paid",
        "tax_deducted", "tds_deposited", "pan", "month_key", "month_label", "financial_year", "quarter_label",
      ],
      parsed.transactions.map((row) => ({
        id: uuid(),
        project_id: projectId,
        import_batch_id: batchId,
        source_file_name: file.originalname,
        company_pan: row.companyPan,
        tan: row.tan,
        deductor_name: row.deductorName,
        section_code: row.sectionCode,
        transaction_date: row.transactionDate,
        booking_status: row.bookingStatus,
        booking_date: row.bookingDate,
        remarks: row.remarks,
        amount_paid: row.amountPaid,
        tax_deducted: row.taxDeducted,
        tds_deposited: row.tdsDeposited,
        pan: row.pan,
        month_key: row.monthKey,
        month_label: row.monthLabel,
        financial_year: row.financialYear,
        quarter_label: row.quarterLabel,
      })),
    );

    await client.query(
      "update import_batches set row_count = $2, metadata = $3 where id = $1",
      [batchId, parsed.transactions.length, { filePan: parsed.filePan, deductors: parsed.deductors.length }],
    );

    return batchId;
  });

  return {
    projectId,
    importBatchId,
    filePan: parsed.filePan,
    deductors: parsed.deductors.length,
    transactions: parsed.transactions.length,
  };
}

async function runReconciliation(projectId, payload) {
  const project = await ensureProject(projectId);
  const tolerance = payload.tolerance === undefined ? Number(project.tolerance) : Number(payload.tolerance);
  const onlyFinal = payload.onlyFinal === undefined ? project.only_final : Boolean(payload.onlyFinal);
  const payloadMappings = [];
  if (payload.ccMap && typeof payload.ccMap === "object" && !Array.isArray(payload.ccMap)) {
    Object.entries(payload.ccMap).forEach(([companyCode, companyPan]) => {
      if (companyCode && companyPan) {
        payloadMappings.push({
          companyCode: String(companyCode).trim().toUpperCase(),
          companyPan: String(companyPan).trim().toUpperCase(),
        });
      }
    });
  }
  if (Array.isArray(payload.mappings)) {
    payload.mappings.forEach((mapping) => {
      if (mapping.companyCode && mapping.companyPan) {
        payloadMappings.push({
          companyCode: String(mapping.companyCode).trim().toUpperCase(),
          companyPan: String(mapping.companyPan).trim().toUpperCase(),
        });
      }
    });
  }

  const [mappingRows, glRowsResult, tasRowsResult, masterLookup] = await Promise.all([
    query("select company_code, company_pan from company_code_mappings where project_id = $1", [projectId]),
    query("select * from gl_entries where project_id = $1 order by posting_date nulls last, id", [projectId]),
    query("select * from tas_transactions where project_id = $1 order by transaction_date nulls last, id", [projectId]),
    loadMasterLookup(projectId),
  ]);

  if (mappingRows.rowCount === 0 && payloadMappings.length > 0) {
    await upsertCompanyCodeMappings(projectId, { mappings: payloadMappings });
    mappingRows.rows.push(...payloadMappings.map((mapping) => ({
      company_code: mapping.companyCode,
      company_pan: mapping.companyPan,
    })));
    mappingRows.rowCount = payloadMappings.length;
  }

  if (mappingRows.rowCount === 0) {
    throw badRequest("No company code mappings found for this project.");
  }

  const companyCodeToPan = new Map(mappingRows.rows.map((row) => [row.company_code, row.company_pan]));
  const glByCompany = new Map();
  const routingExceptions = {
    blankCompanyCode: [],
    unknownCompanyCode: [],
  };

  for (const row of glRowsResult.rows) {
    const companyCode = String(row.company_code || "").trim().toUpperCase();
    if (!companyCode) {
      routingExceptions.blankCompanyCode.push(row);
      continue;
    }

    const companyPan = companyCodeToPan.get(companyCode);
    if (!companyPan) {
      routingExceptions.unknownCompanyCode.push(row);
      continue;
    }

    row.company_pan = companyPan;
    if (!glByCompany.has(companyPan)) {
      glByCompany.set(companyPan, []);
    }
    glByCompany.get(companyPan).push(row);
  }

  const tasByCompany = new Map();
  for (const row of tasRowsResult.rows) {
    const companyPan = row.company_pan;
    if (!companyPan) {
      continue;
    }

    if (!tasByCompany.has(companyPan)) {
      tasByCompany.set(companyPan, []);
    }
    tasByCompany.get(companyPan).push(row);
  }

  const companyPans = new Set([...glByCompany.keys(), ...tasByCompany.keys()]);
  if (companyPans.size === 0) {
    throw badRequest("No routable GL rows or 26AS rows found for this project.");
  }

  const runId = uuid();
  const companySummaries = [];
  const allReconRows = [];
  const allExceptions = [];

  await withTransaction(async (client) => {
    await client.query("delete from reconciliation_results where project_id = $1", [projectId]);
    await client.query("delete from reconciliation_exceptions where project_id = $1", [projectId]);
    await client.query("delete from reconciliation_runs where project_id = $1", [projectId]);

    await client.query(
      `insert into reconciliation_runs (id, project_id, tolerance, only_final, summary)
       values ($1, $2, $3, $4, $5)`,
      [
        runId,
        projectId,
        tolerance,
        onlyFinal,
        JSON.stringify({
          status: "running",
          companies: [],
          totalReconRows: 0,
          totalExceptions: 0,
        }),
      ],
    );

    for (const companyPan of companyPans) {
      const companyResult = reconcileCompany({
        companyPan,
        glRows: glByCompany.get(companyPan) || [],
        tasRows: tasByCompany.get(companyPan) || [],
        panMetadataByPan: masterLookup.panMetadataByPan,
        tolerance,
        onlyFinal,
      });

      const companySummary = {
        companyPan,
        glRows: (glByCompany.get(companyPan) || []).length,
        tasRows: (tasByCompany.get(companyPan) || []).length,
        reconRows: companyResult.reconRows.length,
        panSummaryRows: companyResult.panSummary.length,
      };
      companySummaries.push(companySummary);
      allReconRows.push(...companyResult.reconRows);

      await bulkInsert(
        client,
        "reconciliation_results",
        [
          "id", "reconciliation_run_id", "project_id", "company_pan", "pan", "month_key", "month_label",
          "financial_year", "quarter_label", "customer_name", "region", "salesman", "rating", "gl_amount",
          "tas_tds", "difference_amount", "gl_count", "tas_count", "gl_tans", "tas_tans", "tan_check",
          "sections", "assignments", "status",
        ],
        companyResult.reconRows.map((row) => ({
          id: uuid(),
          reconciliation_run_id: runId,
          project_id: projectId,
          company_pan: row.companyPan,
          pan: row.pan,
          month_key: row.monthKey,
          month_label: row.monthLabel,
          financial_year: row.financialYear,
          quarter_label: row.quarterLabel,
          customer_name: row.customerName,
          region: row.region,
          salesman: row.salesman,
          rating: row.rating,
          gl_amount: row.glAmount,
          tas_tds: row.tasTds,
          difference_amount: row.differenceAmount,
          gl_count: row.glCount,
          tas_count: row.tasCount,
          gl_tans: row.glTans,
          tas_tans: row.tasTans,
          tan_check: row.tanCheck,
          sections: row.sections,
          assignments: row.assignments,
          status: row.status,
        })),
      );

      const exceptionEntries = [
        { type: "gl_unmapped_assignments", payload: companyResult.exceptions.glUnmappedAssignments, companyPan },
        { type: "tas_unmapped_tans", payload: companyResult.exceptions.tasUnmappedTans, companyPan },
        { type: "tan_cross_check_mismatches", payload: companyResult.exceptions.tanCrossCheckMismatches, companyPan },
        { type: "tas_non_final", payload: companyResult.exceptions.nonFinalTransactions, companyPan },
      ];

      for (const exceptionEntry of exceptionEntries) {
        allExceptions.push(exceptionEntry);
        await client.query(
          `insert into reconciliation_exceptions (id, reconciliation_run_id, project_id, company_pan, exception_type, payload)
           values ($1, $2, $3, $4, $5, $6)`,
          [uuid(), runId, projectId, exceptionEntry.companyPan, exceptionEntry.type, JSON.stringify(exceptionEntry.payload)],
        );
      }
    }

    const globalExceptions = [
      {
        type: "blank_company_code",
        payload: routingExceptions.blankCompanyCode,
      },
      {
        type: "unknown_company_code",
        payload: routingExceptions.unknownCompanyCode,
      },
    ];

    for (const exceptionEntry of globalExceptions) {
      allExceptions.push(exceptionEntry);
      await client.query(
        `insert into reconciliation_exceptions (id, reconciliation_run_id, project_id, company_pan, exception_type, payload)
         values ($1, $2, $3, $4, $5, $6)`,
        [uuid(), runId, projectId, null, exceptionEntry.type, JSON.stringify(exceptionEntry.payload)],
      );
    }

    await client.query(
      `update reconciliation_runs
       set summary = $2
       where id = $1`,
      [
        runId,
        JSON.stringify({
          status: "completed",
          companies: companySummaries,
          totalReconRows: allReconRows.length,
          totalExceptions: allExceptions.length,
        }),
      ],
    );
  });

  return {
    projectId,
    reconciliationRunId: runId,
    tolerance,
    onlyFinal,
    companies: companySummaries,
    totalReconRows: allReconRows.length,
  };
}

async function listProjects() {
  const result = await query(
    `select id, name, client_name, financial_year, tolerance, only_final, created_at
     from projects
     order by created_at desc`,
    [],
  );
  return result.rows.map((row) => ({
    id: row.id,
    name: row.name,
    clientName: row.client_name,
    financialYear: row.financial_year,
    tolerance: Number(row.tolerance),
    onlyFinal: row.only_final,
    createdAt: row.created_at,
  }));
}

async function getLatestRun(projectId) {
  const result = await query(
    "select * from reconciliation_runs where project_id = $1 order by created_at desc limit 1",
    [projectId],
  );
  if (result.rowCount === 0) {
    throw notFound("No reconciliation run found for this project.");
  }
  return result.rows[0];
}

async function getProjectSummary(projectId) {
  await ensureProject(projectId);
  const latestRun = await getLatestRun(projectId);

  const totalsResult = await query(
    `select
       coalesce(round(sum(gl_amount)::numeric, 2), 0)         as gl_total,
       coalesce(round(sum(tas_tds)::numeric, 2), 0)           as tas_total,
       coalesce(round(sum(difference_amount)::numeric, 2), 0) as variance,
       count(*)::int                                           as gl_row_count,
       sum(tas_count)::int                                     as tas_row_count,
       count(distinct pan)::int                               as pan_count
     from reconciliation_results
     where project_id = $1 and reconciliation_run_id = $2`,
    [projectId, latestRun.id],
  );

  const t = totalsResult.rows[0] || {};

  return {
    projectId,
    reconciliationRunId: latestRun.id,
    tolerance: Number(latestRun.tolerance),
    onlyFinal: latestRun.only_final,
    summary: latestRun.summary,
    totals: {
      glTotal:     Number(t.gl_total    || 0),
      tasTotal:    Number(t.tas_total   || 0),
      variance:    Number(t.variance    || 0),
      glRowCount:  Number(t.gl_row_count  || 0),
      tasRowCount: Number(t.tas_row_count || 0),
      panCount:    Number(t.pan_count   || 0),
    },
  };
}

async function getPanSummary(projectId, queryParams) {
  await ensureProject(projectId);
  const latestRun = await getLatestRun(projectId);

  const clauses = ["project_id = $1", "reconciliation_run_id = $2"];
  const params = [projectId, latestRun.id];
  let paramIndex = 3;

  if (queryParams.companyPan) {
    clauses.push(`company_pan = $${paramIndex}`);
    params.push(String(queryParams.companyPan).trim().toUpperCase());
    paramIndex += 1;
  }

  const result = await query(
    `select
       company_pan,
       pan,
       max(customer_name) as customer_name,
       max(region) as region,
       max(salesman) as salesman,
       max(rating) as rating,
       round(sum(gl_amount)::numeric, 2) as gl_amount,
       round(sum(tas_tds)::numeric, 2) as tas_tds,
       round(sum(difference_amount)::numeric, 2) as difference_amount,
       sum(gl_count) as gl_count,
       sum(tas_count) as tas_count,
       count(*) as month_count
     from reconciliation_results
     where ${clauses.join(" and ")}
     group by company_pan, pan
     order by abs(sum(difference_amount)) desc, pan asc`,
    params,
  );

  return {
    projectId,
    reconciliationRunId: latestRun.id,
    rows: result.rows,
  };
}

async function getReconRows(projectId, queryParams) {
  await ensureProject(projectId);
  const latestRun = await getLatestRun(projectId);

  const clauses = ["project_id = $1", "reconciliation_run_id = $2"];
  const params = [projectId, latestRun.id];
  let paramIndex = 3;

  if (queryParams.companyPan) {
    clauses.push(`company_pan = $${paramIndex}`);
    params.push(String(queryParams.companyPan).trim().toUpperCase());
    paramIndex += 1;
  }
  if (queryParams.pan) {
    clauses.push(`pan = $${paramIndex}`);
    params.push(String(queryParams.pan).trim().toUpperCase());
    paramIndex += 1;
  }
  if (queryParams.status) {
    clauses.push(`status = $${paramIndex}`);
    params.push(String(queryParams.status).trim());
    paramIndex += 1;
  }

  const result = await query(
    `select *
     from reconciliation_results
     where ${clauses.join(" and ")}
     order by abs(difference_amount) desc, month_key asc`,
    params,
  );

  return {
    projectId,
    reconciliationRunId: latestRun.id,
    rows: result.rows,
  };
}

async function getExceptions(projectId) {
  await ensureProject(projectId);
  const latestRun = await getLatestRun(projectId);
  const result = await query(
    `select company_pan, exception_type, payload
     from reconciliation_exceptions
     where project_id = $1 and reconciliation_run_id = $2
     order by company_pan nulls first, exception_type asc`,
    [projectId, latestRun.id],
  );

  return {
    projectId,
    reconciliationRunId: latestRun.id,
    rows: result.rows,
  };
}

async function getFrontendState(projectId) {
  const project = await ensureProject(projectId);
  const [mappingRows, glRowsResult, tasRowsResult, masterLookup] = await Promise.all([
    query("select company_code, company_pan from company_code_mappings where project_id = $1", [projectId]),
    query("select * from gl_entries where project_id = $1 order by posting_date nulls last, id", [projectId]),
    query("select * from tas_transactions where project_id = $1 order by transaction_date nulls last, id", [projectId]),
    loadMasterLookup(projectId),
  ]);

  const ccMap = {};
  mappingRows.rows.forEach((row) => {
    ccMap[row.company_code] = row.company_pan;
  });

  const companies = {};
  const glByCompany = {};
  const tasByCompany = {};
  const glBlankCC = [];
  const glUnknownCC = [];
  const tasNonFinalByCompany = {};

  glRowsResult.rows.forEach((row) => {
    const companyCode = String(row.company_code || "").trim().toUpperCase();
    if (!companyCode) {
      glBlankCC.push({
        docNo: row.document_number,
        postingDate: row.posting_date,
        amount: Number(row.amount),
        assignment: row.assignment,
        tanBook: row.tan_book_raw,
        text: row.text_value,
        sourceFile: row.source_file_name,
        issue: "Blank Company Code - cannot route to any company",
      });
      return;
    }

    const companyPan = ccMap[companyCode];
    if (!companyPan) {
      glUnknownCC.push({
        companyCode,
        docNo: row.document_number,
        postingDate: row.posting_date,
        amount: Number(row.amount),
        assignment: row.assignment,
        tanBook: row.tan_book_raw,
        text: row.text_value,
        sourceFile: row.source_file_name,
        issue: `Company Code "${companyCode}" not in CC map - update the map or remove these rows`,
      });
      return;
    }

    if (!glByCompany[companyPan]) glByCompany[companyPan] = [];
    glByCompany[companyPan].push(row);
  });

  tasRowsResult.rows.forEach((row) => {
    if (!row.company_pan) return;
    if (!tasByCompany[row.company_pan]) tasByCompany[row.company_pan] = [];
    tasByCompany[row.company_pan].push(row);
    if (project.only_final && row.booking_status !== "F") {
      if (!tasNonFinalByCompany[row.company_pan]) tasNonFinalByCompany[row.company_pan] = [];
      tasNonFinalByCompany[row.company_pan].push(row);
    }
  });

  const companyPans = new Set([...Object.keys(glByCompany), ...Object.keys(tasByCompany)]);
  companyPans.forEach((companyPan) => {
    const companyResult = reconcileCompany({
      companyPan,
      glRows: glByCompany[companyPan] || [],
      tasRows: tasByCompany[companyPan] || [],
      panMetadataByPan: masterLookup.panMetadataByPan,
      tolerance: Number(project.tolerance),
      onlyFinal: project.only_final,
    });

    companies[companyPan] = {
      recon: companyResult.reconRows.map((row) => ({
        pan: row.pan,
        monthKey: row.monthKey,
        monthLabel: row.monthLabel,
        fy: row.financialYear,
        quarter: row.quarterLabel,
        customer: row.customerName,
        region: row.region,
        salesman: row.salesman,
        rating: row.rating,
        glAmount: row.glAmount,
        tasTds: row.tasTds,
        diff: row.differenceAmount,
        glCount: row.glCount,
        tasCount: row.tasCount,
        glTans: row.glTans,
        tasTans: row.tasTans,
        tanCheck: row.tanCheck,
        sections: row.sections,
        assignments: row.assignments,
        status: row.status,
      })),
      reports: companyResult.reports,
      exceptions: companyResult.exceptions,
      glRowsCount: (glByCompany[companyPan] || []).length,
      tasRowsCount: project.only_final
        ? (tasByCompany[companyPan] || []).filter((row) => row.booking_status === "F").length
        : (tasByCompany[companyPan] || []).length,
    };
  });

  return {
    projectId,
    project: {
      id: project.id,
      name: project.name,
      clientName: project.client_name,
      financialYear: project.financial_year,
      tolerance: Number(project.tolerance),
      onlyFinal: project.only_final,
    },
    master: {
      agr2pan: Object.fromEntries(masterLookup.agreementToPan),
      tan2pan: Object.fromEntries(masterLookup.tanToPan),
      panMeta: Object.fromEntries([...masterLookup.panMetadataByPan.entries()].map(([pan, meta]) => [pan, {
        name: meta.customerName || "",
        region: meta.region || "",
        salesman: meta.salesman || "",
        customer: meta.exposureCustomerName || meta.customerName || "",
        rating: meta.rating || "",
      }])),
    },
    gl: glRowsResult.rows.map((row) => ({
      sourceFile: row.source_file_name,
      account: row.account,
      assignment: row.assignment,
      docNo: row.document_number,
      companyCode: row.company_code,
      postingDate: row.posting_date,
      documentDate: row.document_date,
      amount: Number(row.amount),
      localCurrency: row.local_currency,
      text: row.text_value,
      reference: row.reference,
      tanBook: row.tan_book,
      tanBookRaw: row.tan_book_raw,
      pan: row.pan,
      monthKey: row.month_key,
      monthLabel: row.month_label,
      fy: row.financial_year,
      quarter: row.quarter_label,
    })),
    tas: Object.fromEntries(Object.entries(tasByCompany).map(([companyPan, rows]) => [companyPan, rows.map((row) => ({
      sourceFile: row.source_file_name,
      tan: row.tan,
      deductor: row.deductor_name,
      section: row.section_code,
      txnDate: row.transaction_date,
      bookingStatus: row.booking_status,
      amtPaid: Number(row.amount_paid),
      tds: Number(row.tax_deducted),
      tdsDeposited: Number(row.tds_deposited),
      pan: row.pan,
      monthKey: row.month_key,
      monthLabel: row.month_label,
      fy: row.financial_year,
      quarter: row.quarter_label,
    }))])),
    results: {
      companies,
      ccMap,
      globalExceptions: {
        glBlankCC,
        glUnknownCC,
        tasNonFinalByCompany,
        ccMap,
        onlyFinalFilter: project.only_final,
      },
    },
  };
}

module.exports = {
  createProject,
  listProjects,
  upsertCompanyCodeMappings,
  importMasterWorkbook,
  importGlWorkbook,
  import26AsText,
  runReconciliation,
  getProjectSummary,
  getPanSummary,
  getReconRows,
  getExceptions,
  getFrontendState,
};
