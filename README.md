# End-to-End Gmail Job Opportunity Pipeline on Databricks

An automated data engineering pipeline built on **Databricks** that incrementally ingests emails from Gmail, classifies job opportunities using AI functions, tracks state in **Delta Lake** under **Unity Catalog**, and runs on a schedule using **Serverless Compute**.

---

## Architecture & Data Flow

1. **Incremental Gmail Ingestion (`gmail_job_opportunity_pipeline.ipynb`)**
   - Authenticates to the Gmail API using OAuth 2.0 credentials retrieved at runtime via `dbutils.secrets.get()`.
   - Queries messages with day-level `after:` search and filters precisely on `internalDate` against a watermark so re-runs remain idempotent without skipping or duplicating messages.
   - Writes raw message records into `workspace.gmail.messages`.

2. **AI Classification & Incremental Delta Merge**[cite: 3]
   - Uses `ai_classify` over recent messages (`received_at > current_timestamp() - INTERVAL 25 HOURS`) to label emails as `'job_opportunity'` or `'other'`[cite: 3].
   - Uses a `LEFT ANTI JOIN` against `workspace.gmail.message_classifications` and a Delta `MERGE` (`WHEN NOT MATCHED THEN INSERT`) to avoid re-classifying already processed emails[cite: 3].
   - Surfaces filtered opportunities into `workspace.gmail.new_job_opportunities` with visible upstream/downstream table lineage.

3. **Orchestration & Version Control**[cite: 2, 3, 4]
   - Managed in a Databricks Git folder and pushed to GitHub.
   - Scheduled as a Databricks Job (`Gmail_JobOppurtunity_datapipeline`) running on **Serverless** compute (`Every hour`).

---

## Step 1: Extract OAuth Credentials & Refresh Token

Because personal Gmail accounts require OAuth 2.0 rather than a plain account password, generate your `client_id`, `client_secret`, and `refresh_token` locally:

1. **Create a Google Cloud Project & Enable Gmail API**
   - Open [Google Cloud Console](https://console.cloud.google.com/) and create a project.
   - Navigate to **APIs & Services → Library**, search for **Gmail API**, and click **Enable**.

2. **Configure OAuth Consent Screen & Add Test User**
   - Set up the OAuth consent screen (`External` for a personal `@gmail.com` account).
   - Add your personal Gmail address under **Test users** (required while the app is in Testing mode to avoid `403 access_denied`).
   - *(Optional)* Publish the app if you want to avoid 7-day refresh token expiration while testing personal use.

3. **Create Desktop OAuth Client & Download JSON**
   - Go to **APIs & Services → Credentials → Create Credentials → OAuth client ID**.
   - Select **Desktop app**, create the client, and download the JSON file (`client_secret_*.json`).

4. **Generate `refresh_token` in a Local Virtual Environment**
   - On Linux systems enforcing PEP 668, use a virtual environment rather than system-wide `pip`:
     ```bash
     python3 -m venv ~/gmail-auth-env
     source ~/gmail-auth-env/bin/activate
     pip install google-auth-oauthlib google-api-python-client
     ```
   - Run your local authorization script using `client_secret.json` with read-only Gmail scope (`https://www.googleapis.com/auth/gmail.readonly`) and complete the browser sign-in flow to obtain:
     - `client_id`
     - `client_secret`
     - `refresh_token`
   - Once captured, clean up the temporary local environment if no longer needed:
     ```bash
     deactivate
     rm -rf ~/gmail-auth-env
     ```

---

## Step 2: Store Credentials as Databricks Secrets

Never hardcode tokens or client secrets inside the notebook or commit them to Git. Store them in **Databricks Unity Catalog Secrets** (or a classic secret scope):

### Option A: Unity Catalog Secrets (Recommended for Unity Catalog / Free Edition)
1. In Databricks, open **Catalog** → select catalog `workspace` and schema `default` (or your target schema)[cite: 3].
2. Create three secrets with these exact key names:
   - `client_id`
   - `client_secret`
   - `refresh_token`
3. Read them inside your notebook via `dbutils.secrets.get(...)`:
   ```python
   GMAIL_CLIENT_ID     = dbutils.secrets.get(catalog="workspace", schema="default", key="client_id")
   GMAIL_CLIENT_SECRET = dbutils.secrets.get(catalog="workspace", schema="default", key="client_secret")
   GMAIL_REFRESH_TOKEN = dbutils.secrets.get(catalog="workspace", schema="default", key="refresh_token")