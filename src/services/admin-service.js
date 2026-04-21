const { query } = require("../db");

async function getAdminDashboard() {
  const result = await query(`
    with latest_runs as (
      select distinct on (project_id)
        id,
        project_id,
        tolerance,
        only_final,
        summary,
        created_at
      from reconciliation_runs
      order by project_id, created_at desc
    ),
    import_counts as (
      select
        project_id,
        count(*) filter (where import_type = 'master')::int as master_files,
        count(*) filter (where import_type = 'gl')::int as gl_files,
        count(*) filter (where import_type = '26as')::int as tas_files
      from import_batches
      group by project_id
    ),
    recon_totals as (
      select
        project_id,
        reconciliation_run_id,
        count(*)::int as recon_rows,
        count(distinct company_pan)::int as company_count,
        count(distinct pan)::int as pan_count,
        coalesce(round(sum(gl_amount)::numeric, 2), 0) as gl_amount,
        coalesce(round(sum(tas_tds)::numeric, 2), 0) as tas_tds,
        coalesce(round(sum(difference_amount)::numeric, 2), 0) as variance,
        count(*) filter (where status like 'Perfect Match%')::int as perfect_count,
        count(*) filter (where status = 'Amount Mismatch')::int as mismatch_count,
        count(*) filter (where status = 'GL Only')::int as gl_only_count,
        count(*) filter (where status = '26AS Only')::int as tas_only_count
      from reconciliation_results
      group by project_id, reconciliation_run_id
    )
    select
      p.id as project_id,
      p.name,
      p.client_name,
      p.financial_year,
      p.created_at,
      lr.id as reconciliation_run_id,
      lr.created_at as reconciled_at,
      coalesce(ic.master_files, 0) as master_files,
      coalesce(ic.gl_files, 0) as gl_files,
      coalesce(ic.tas_files, 0) as tas_files,
      coalesce(rt.recon_rows, 0) as recon_rows,
      coalesce(rt.company_count, 0) as company_count,
      coalesce(rt.pan_count, 0) as pan_count,
      coalesce(rt.gl_amount, 0) as gl_amount,
      coalesce(rt.tas_tds, 0) as tas_tds,
      coalesce(rt.variance, 0) as variance,
      coalesce(rt.perfect_count, 0) as perfect_count,
      coalesce(rt.mismatch_count, 0) as mismatch_count,
      coalesce(rt.gl_only_count, 0) as gl_only_count,
      coalesce(rt.tas_only_count, 0) as tas_only_count
    from projects p
    left join latest_runs lr on lr.project_id = p.id
    left join import_counts ic on ic.project_id = p.id
    left join recon_totals rt on rt.project_id = p.id and rt.reconciliation_run_id = lr.id
    order by p.created_at desc
  `);

  const rows = result.rows.map((row) => ({
    projectId: row.project_id,
    reconciliationRunId: row.reconciliation_run_id,
    name: row.name,
    clientName: row.client_name,
    financialYear: row.financial_year,
    createdAt: row.created_at,
    reconciledAt: row.reconciled_at,
    masterFiles: Number(row.master_files || 0),
    glFiles: Number(row.gl_files || 0),
    tasFiles: Number(row.tas_files || 0),
    reconRows: Number(row.recon_rows || 0),
    companyCount: Number(row.company_count || 0),
    panCount: Number(row.pan_count || 0),
    glAmount: Number(row.gl_amount || 0),
    tasTds: Number(row.tas_tds || 0),
    variance: Number(row.variance || 0),
    perfectCount: Number(row.perfect_count || 0),
    mismatchCount: Number(row.mismatch_count || 0),
    glOnlyCount: Number(row.gl_only_count || 0),
    tasOnlyCount: Number(row.tas_only_count || 0),
  }));

  return {
    rows,
    totals: rows.reduce((acc, row) => {
      acc.projects += 1;
      acc.reconRows += row.reconRows;
      acc.glAmount += row.glAmount;
      acc.tasTds += row.tasTds;
      acc.variance += row.variance;
      return acc;
    }, { projects: 0, reconRows: 0, glAmount: 0, tasTds: 0, variance: 0 }),
  };
}

module.exports = {
  getAdminDashboard,
};
