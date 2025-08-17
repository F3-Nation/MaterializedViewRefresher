# GCP Materialized View Refresh Job

This project provides a Job that automatically refreshes PostgreSQL materialized views on a schedule.  

It searches every schema in Postgres database for a table named `materializedviews`.  
If found, it uses the table’s configuration to decide which materialized views to refresh and at what hours.

## How It Works

1. The job runs hourly.
2. It connects to the PostgreSQL database.
3. For each schema:
   - If a table named `materializedviews` exists, the job queries it.
   - The table must contain:
     - `name` (the name of the materialized view to refresh)
     - `hours` (a comma-delimited list of hours `0–23` in UTC when the refresh should run)
4. If the current UTC hour matches one of the values in `hours`, the job executes:
   ```sql
   REFRESH MATERIALIZED VIEW <schema>.<name>;
   ```
5. Logs are written to Cloud Logging when:
    - A MaterializedViews table is found
    - Each row is checked
    - A refresh starts
    - A refresh completes (or fails)

**Important Notes**
- The hours column must contain UTC hours (not local time).
- If a view should refresh every hour, you can either list all 24 hours or just automate in your schema setup.
- The job assumes the name matches exactly the materialized view in that schema.

## Creating the Table
```sql
-- Create the control table in your schema
CREATE TABLE myschema.MaterializedViews (
    name   text PRIMARY KEY,  -- The name of the materialized view in this schema (unique)
    hours  text NOT NULL      -- Comma-delimited list of hours (UTC, 0–23)
);
```

## Example of Populated Table
```sql
-- Add entries for each materialized view you want refreshed
INSERT INTO myschema.MaterializedViews (name, hours)
VALUES
    ('daily_sales_summary', '0,6,12,18'),  -- refresh at midnight, 6am, noon, 6pm UTC
    ('user_activity_rollup', '1,13'),      -- refresh at 1am and 1pm UTC
    ('hourly_metrics', '0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23'); -- every hour
```
# MaterializedViewRefresher
