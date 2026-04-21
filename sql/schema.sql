create extension if not exists "uuid-ossp";

create table if not exists projects (
  id uuid primary key,
  name text not null,
  client_name text,
  financial_year text,
  tolerance numeric(14,2) not null default 10,
  only_final boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists company_code_mappings (
  id uuid primary key,
  project_id uuid not null references projects(id) on delete cascade,
  company_code text not null,
  company_pan text not null,
  created_at timestamptz not null default now(),
  unique(project_id, company_code)
);

create table if not exists import_batches (
  id uuid primary key,
  project_id uuid not null references projects(id) on delete cascade,
  import_type text not null check (import_type in ('master', 'gl', '26as')),
  file_name text not null,
  file_size_bytes bigint,
  row_count integer not null default 0,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create table if not exists master_agreement_pan (
  id uuid primary key,
  project_id uuid not null references projects(id) on delete cascade,
  agreement_code text not null,
  pan text,
  import_batch_id uuid references import_batches(id) on delete set null,
  created_at timestamptz not null default now(),
  unique(project_id, agreement_code)
);

create table if not exists master_tan_pan (
  id uuid primary key,
  project_id uuid not null references projects(id) on delete cascade,
  tan text not null,
  pan text,
  import_batch_id uuid references import_batches(id) on delete set null,
  created_at timestamptz not null default now(),
  unique(project_id, tan)
);

create table if not exists master_pan_metadata (
  id uuid primary key,
  project_id uuid not null references projects(id) on delete cascade,
  pan text not null,
  customer_name text,
  region text,
  salesman text,
  exposure_customer_name text,
  rating text,
  import_batch_id uuid references import_batches(id) on delete set null,
  created_at timestamptz not null default now(),
  unique(project_id, pan)
);

create table if not exists gl_entries (
  id uuid primary key,
  project_id uuid not null references projects(id) on delete cascade,
  import_batch_id uuid references import_batches(id) on delete set null,
  source_file_name text not null,
  account text,
  assignment text,
  document_number text,
  company_code text,
  company_pan text,
  posting_date date,
  document_date date,
  amount numeric(18,2) not null,
  local_currency text,
  text_value text,
  reference text,
  tan_book text,
  tan_book_raw text,
  pan text,
  month_key text,
  month_label text,
  financial_year text,
  quarter_label text,
  created_at timestamptz not null default now()
);

create index if not exists idx_gl_entries_project_pan_month on gl_entries(project_id, pan, month_key);
create index if not exists idx_gl_entries_project_company_pan on gl_entries(project_id, company_pan);

create table if not exists tas_transactions (
  id uuid primary key,
  project_id uuid not null references projects(id) on delete cascade,
  import_batch_id uuid references import_batches(id) on delete set null,
  source_file_name text not null,
  company_pan text,
  tan text not null,
  deductor_name text,
  section_code text,
  transaction_date date,
  booking_status text,
  booking_date date,
  remarks text,
  amount_paid numeric(18,2) not null default 0,
  tax_deducted numeric(18,2) not null default 0,
  tds_deposited numeric(18,2) not null default 0,
  pan text,
  month_key text,
  month_label text,
  financial_year text,
  quarter_label text,
  created_at timestamptz not null default now()
);

create index if not exists idx_tas_transactions_project_pan_month on tas_transactions(project_id, pan, month_key);
create index if not exists idx_tas_transactions_project_company_pan on tas_transactions(project_id, company_pan);

create table if not exists reconciliation_runs (
  id uuid primary key,
  project_id uuid not null references projects(id) on delete cascade,
  tolerance numeric(14,2) not null,
  only_final boolean not null,
  summary jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create table if not exists reconciliation_results (
  id uuid primary key,
  reconciliation_run_id uuid not null references reconciliation_runs(id) on delete cascade,
  project_id uuid not null references projects(id) on delete cascade,
  company_pan text not null,
  pan text not null,
  month_key text not null,
  month_label text,
  financial_year text,
  quarter_label text,
  customer_name text,
  region text,
  salesman text,
  rating text,
  gl_amount numeric(18,2) not null default 0,
  tas_tds numeric(18,2) not null default 0,
  difference_amount numeric(18,2) not null default 0,
  gl_count integer not null default 0,
  tas_count integer not null default 0,
  gl_tans text,
  tas_tans text,
  tan_check text,
  sections text,
  assignments text,
  status text not null,
  created_at timestamptz not null default now()
);

create index if not exists idx_reconciliation_results_run_company on reconciliation_results(reconciliation_run_id, company_pan);
create index if not exists idx_reconciliation_results_run_pan on reconciliation_results(reconciliation_run_id, pan);

create table if not exists reconciliation_exceptions (
  id uuid primary key,
  reconciliation_run_id uuid not null references reconciliation_runs(id) on delete cascade,
  project_id uuid not null references projects(id) on delete cascade,
  company_pan text,
  exception_type text not null,
  payload jsonb not null,
  created_at timestamptz not null default now()
);

create table if not exists app_users (
  id uuid primary key,
  name text not null unique,
  password_hash text not null,
  role text not null default 'admin',
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
