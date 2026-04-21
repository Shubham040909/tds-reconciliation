# TDS Reconciliation API

Backend scaffold for moving the current browser-only TDS Form 26AS reconciliation tool into Node + PostgreSQL.

## What this backend does

- imports the master workbook
- imports GL Excel files
- imports 26AS text files
- stores normalized raw rows in PostgreSQL
- runs reconciliation on the server
- exposes summary and detail APIs for a future frontend

## Setup

1. Install dependencies

```powershell
npm.cmd install
```

2. Create a PostgreSQL database, or use your Supabase project.

3. Copy `.env.example` to `.env` for local PostgreSQL, or copy `.env.supabase.example` to `.env` for Supabase.

For Supabase, use the PostgreSQL connection string from:

`Supabase Dashboard -> Connect -> Connection string -> Transaction pooler`

Set:

```powershell
DATABASE_SSL=true
```

4. Run the SQL schema from [sql/schema.sql](C:/Users/Admin/Downloads/html/sql/schema.sql:1)

5. Start the API

```powershell
npm.cmd start
```

## Main endpoints

- `POST /api/auth/login`
- `GET /api/admin/dashboard`
- `POST /api/projects`
- `POST /api/projects/:projectId/company-code-map`
- `POST /api/projects/:projectId/import/master`
- `POST /api/projects/:projectId/import/gl`
- `POST /api/projects/:projectId/import/26as`
- `POST /api/projects/:projectId/reconcile`
- `GET /api/projects/:projectId/summary`
- `GET /api/projects/:projectId/pan-summary`
- `GET /api/projects/:projectId/recon`
- `GET /api/projects/:projectId/exceptions`

## Notes

- The current implementation keeps the reconciliation grain at `PAN + month`, matching the HTML tool.
- The APIs are built so the existing static frontend can later be rewired to call the backend.
- The existing HTML page calls this API when you click `Run Reconciliation`, then stores imports and results in PostgreSQL/Supabase.
- Check the database connection with `http://localhost:3000/health/db` before uploading large files.
- Default local admin login is controlled by `ADMIN_USERNAME` and `ADMIN_PASSWORD` in `.env`. Current seeded login is `admin` / `admin123`.
