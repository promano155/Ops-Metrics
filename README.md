# Ops-Metrics

Python scripts and GitHub Actions workflows that keep operations reporting running:
they pull data from Google Sheets, HubSpot, Gmail, and Asana, write metrics to
Supabase for the dashboard, and post digests to Slack.

Everything runs in GitHub Actions. The scripts are not meant to be run on a laptop
unless you are testing, because they need the secrets listed below.

## How things get triggered

| Type | How it runs |
| --- | --- |
| **GitHub cron** | A `schedule:` block in the workflow file (times are UTC). |
| **External cron** | An outside scheduler (cron-job.org) calls GitHub's API to start the workflow, because GitHub's own scheduler can be late or skip runs. The workflow must have `workflow_dispatch`, and **its file name is part of the URL cron-job.org calls**, so renaming or moving a workflow file breaks that job. |
| **Manual** | Run it yourself from the **Actions** tab: pick the workflow, click **Run workflow**. |

## What runs what

### Daily syncs and digests

| Workflow | Script(s) | Trigger | What it does |
| --- | --- | --- | --- |
| `Javi_team_sync.yml` | `sync_data_processing_metrics.py`, `sync_hubspot_ticket_metrics.py`, `sync_hubspot_ticket_sla.py`, `sync_yellow_rows_to_asana.py` | GitHub cron (daily 13:00 UTC) and external cron (hourly) | Main run: data-processing SLA metrics, HubSpot ticket volume and SLA, and flagged billing rows pushed to Asana. The `skip_yellow_row` input leaves out the Asana step. Takes about 11 minutes. **Do not rename this file:** its name is part of the URL cron-job.org calls. |
| `scheduled-data-processing-sync.yml` | `sync_data_processing_metrics.py` | External cron | Daily data-processing SLA metrics. Optional `force_month` input recomputes a closed month. |
| `scheduled-sync-data-issues-to-asana.yml` | `sync_data_issues_to_asana.py` | Not currently triggered | Creates Asana tasks for data issues, file errors, and integration issues from the newest month. Used by the Lovable app; its cron-job.org job is disabled. Kept for its logic. |
| `scheduled-ticket-sla-sync.yml` | `sync_hubspot_ticket_sla.py` | External cron | HubSpot ticket SLA metrics. |
| `scheduled-reconciliations-sync.yml` | `sync_reconciliations_monthly.py` | External cron | Monthly reconciliation volume (opened, completed, backlog), read from email. |
| `scheduled-reports-sent-sync.yml` | `sync_reports_sent_monthly.py` | External cron | Monthly count of client reports sent. |
| `SLA_breach_digest.yml` | `sla_breach_digest.py` | External cron | One Slack message listing tasks currently past their SLA. |
| `daily_completion_digest.yml` | `daily_completions_digest.py` | GitHub cron, Mon-Thu 12:00 UTC | Slack digest: completed this week and month, plus what is in progress and for how long. |
| `javi-friday-digest.yml` | `sync_ops_task_tracker.py` | GitHub cron, Mon-Fri 12:00 UTC | Daily sync of the ad-hoc ops task project, with a Friday-only Slack summary. |

### Manual-only workflows

| Workflow | Script | When to use it |
| --- | --- | --- |
| `manual-data-processing-sync.yml` | `sync_data_processing_metrics.py` | Re-run the data-processing sync on demand. |
| `manual-ticket-sla-sync.yml` | `sync_hubspot_ticket_sla.py` | Re-run the ticket SLA sync on demand. |
| `manual-reconciliations-sync.yml` | `sync_reconciliations_monthly.py` | Re-run the reconciliation sync on demand. |
| `manual-reports-sent-sync.yml` | `sync_reports_sent_monthly.py` | Re-run the reports-sent sync on demand. |
| `manual-sync-data-issues-to-asana.yml` | `sync_data_issues_to_asana.py` | Test the data-issues sync against a specific past month. |
| `cleanup-v1-duplicates.yml` | `cleanup_v1_transition_duplicates.py` | One-time cleanup of duplicate hotel tasks left over from the V1 transition. Runs as a dry run unless `confirm` is ticked. Safe to delete once the cleanup is done. |
| `sync-asana-only.yml` | `sync_yellow_rows_to_asana.py` | External cron (hourly, weekdays) | Pushes flagged billing rows from the billing sheet to Asana without the rest of the main run. Can also be run by hand. |

## Settings the workflows depend on

Set these under **Settings > Secrets and variables > Actions**.

**Secrets**

| Name | Used for |
| --- | --- |
| `SUPABASE_URL`, `SUPABASE_SERVICE_KEY` | Writing metrics to the dashboard database. |
| `ASANA_PAT` | Reading and writing Asana tasks. |
| `HUBSPOT_PRIVATE_APP_TOKEN` | Reading HubSpot tickets. |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | Reading the Google Sheets the syncs use. |
| `GMAIL_OAUTH_CLIENT_ID`, `GMAIL_OAUTH_CLIENT_SECRET`, `GMAIL_OAUTH_REFRESH_TOKEN` | Reading reconciliation emails. |
| `SLACK_BOT_TOKEN` | Posting digests. |

## Rules of thumb

- **Never put a token, key, or password in a script.** Always add it as an Actions secret
  and read it from the environment. Rotate anything that was ever committed, even briefly.
- **Dry-run first** for anything that deletes or bulk-edits. The manual workflows have a
  `dry_run` option; leave it on until the output looks right.
- Every workflow has a `timeout-minutes` limit, so a stuck run stops on its own. If a
  legitimate run gets cancelled for taking too long, raise the limit in that workflow file.
