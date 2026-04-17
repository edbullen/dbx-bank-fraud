# Databricks Demo with Retail Bank Fraud Data

Ingest multiple data-sources and join them to analyze simulated bank fraud transactions and build a machine learning model to predict fraud.

The dataset is based on the [PaySim](https://www.msc-les.org/proceedings/emss/2016/EMSS2016_249.pdf),  dataset (GitHub [repo](https://github.com/EdgarLopezPhD/PaySim) and [LICENSE](https://github.com/EdgarLopezPhD/PaySim/blob/master/LICENSE) for more details). The Databricks demo version augmented this dataset to introduce simulated geo-data as well.  The Databricks version of this data has a [LICENCE](https://github.com/databricks-demos/dbdemos-dataset/blob/main/LICENSE) for use.

![Dashboard](./doc/dashboard_example.png)

## Data Schema

Files are staged in Unity Catalog volumes and then ingested into Delta Tables.  

![Schema](./doc/retail_fraud_schema.png) 

# IDE Connect to the workspace environment

Most steps described can be executed from a local IDE running with a local clone of this Git repo.   Use the Databricks SDK and CLI to authenticate to a remote workspace and it's services.

The Databricks CLI profiles are configured in `~/.databrickscfg`.  

- Check the available CLI profiles to connect to: `databricks auth profiles`
- Authenticate to a configured profile using [U2M OAuth](https://docs.databricks.com/aws/en/dev-tools/cli/authentication#oauth-user-to-machine-u2m-authentication): `databricks auth login -p my_profile_name`.

*Either* 

- set the DEFAULT profile to work in with the Databricks CLI by editing `~/.databrickscfg` and setting the profile name in the `[DEFAULT]` section  
*Or*  
- Add the -p `<my_profile_name>` to the end of all Databricks CLI commands.  
in order  to make sure they get executed against the correct workspace.

# Quick-start deployment
## 1. Raw Data, Volumes, Tables and Views

- Identify / Create the Unity Catalog (UC) Catalog
- Identify / Create the UC Schema
- Create the Volume `bank-fraud` in the UC catalog.schema

Use the `create.sh` and `destroy.sh` scripts to deploy or tear down the demo from the repo root. They support both **non-interactive** (one-shot, suitable for CI or AI agents) and **interactive** (step-by-step prompts) modes.

**Non-interactive (all parameters on the command line):**

```bash
./create.sh --non-interactive --profile myprofile --catalog main --schema default --volume bank-fraud
# ... later, to tear down:
./destroy.sh --non-interactive --profile myprofile --catalog main --schema default --volume bank-fraud
```

**Interactive (prompted for profile, catalog, schema, volume):**

```bash
./create.sh
# or with partial args:
./create.sh --catalog main --schema default
```

**Optional:** sync notebooks to the workspace and create the UC volume if it does not exist:

```bash
./create.sh -y -p myprofile -c main -s default -v bank-fraud --workspace-path /Users/me@example.com/dbx-bank-fraud --create-volume
```

For full options run:

- `./create.sh --help`
- `./destroy.sh --help`

## 2. Deploy Notebooks, ML Model, Serving Endpoint and Dashboard

After the base data and tables are in place (e.g. via `create.sh`), use `deploy.sh` to deploy and **build** the ML artefacts and the Retail Bank Fraud dashboard:

1. **Notebooks** — Imports `fraud_model_training.py`, `fraud_model_deploy.py`, and `fraud_model_run.py` from `./notebooks` into your workspace.
2. **ML build** — Runs the training notebook then the deploy notebook (so the model is registered in Unity Catalog). By default uses **serverless compute** (no cluster ID needed). Optionally pass `--cluster-id` to use an existing cluster. If a notebook run fails, the script prints the run ID and fetches the run output; you can also run `databricks -p <profile> jobs get-run-output <run_id>` to view error details.
3. **Dashboard** — Creates and publishes the Lakeview dashboard from `dashboards/Retail_Bank_Fraud_Dashboard.lvdash.json`, with your catalog and schema applied to the datasets.

NOTE: make sure a folder structure is in place in the workspace for the notebooks to be copied to. This needs to include a pre-created `notebooks` folder, i.e. pre-create `/Users/me@example.com/dbx-bank-fraud/notebooks`

**Example (serverless; no cluster):**

```bash
./deploy.sh -p myprofile -c my_catalog -s my_schema --workspace-path /Users/me@example.com/dbx-bank-fraud --warehouse-id <sql-warehouse-id>  --serve-model
```

**Example (with existing cluster):**

```bash
./deploy.sh -p myprofile -c my_catalog -s my_schema --workspace-path /Users/me@example.com/dbx-bank-fraud --warehouse-id <id> --cluster-id <cluster-id>
```

**Example (non-interactive, serverless):**

```bash
./deploy.sh -y -p myprofile -c my_catalog -s my_schema --workspace-path /Users/me@example.com/dbx-bank-fraud --warehouse-id <id>
```

**Example (with model serving endpoint):**

```bash
./deploy.sh -y -p myprofile -c my_catalog -s my_schema --workspace-path /Users/me@example.com/dbx-bank-fraud --warehouse-id <id> --serve-model
```

**Example (serve-model only; no workspace-path or warehouse-id):**

```bash
./deploy.sh -y -p myprofile -c my_catalog -s my_schema --skip-notebooks --skip-ml --serve-model
```

Example - Use the Databricks CLI to flag a model version `production` then serve that version

```
# Set the production alias on a model version
databricks -p myprofile registered-models set-alias <catalog>.<schema>.bank_fraud_predict production <version_num>
# Deploy "bank-fraud-predict" serving endpoint on bank_fraud_predict@production
./deploy.sh -y -p myprofile -c my_catalog -s my_schema --skip-notebooks --skip-ml --serve-model

```

**Options:**  

- `--skip-ml` — do not run training or deploy notebooks.  
- `--skip-dashboard` — do not create the dashboard.  
- `--skip-notebooks` — do not import notebooks (e.g. dashboard-only or serve-model-only).  
- `--genie` — create a Genie space for `gold_transactions` (warehouse ID required when using `--genie`).  
- `--cluster-id` — optional; omit for serverless.  
- For deploy types that only create the dashboard or `--serve-model` fraud endpoint, use `--skip-notebooks --skip-ml`; then `--workspace-path` and (for serve-model-only) `--warehouse-id` are not required. **`--deploy-agent` always needs `--workspace-path`.**

**Model serving** (when using `--serve-model`):  

- **Endpoint name:** `--endpoint-name` (default `bank-fraud-predict`) is the name of the serving endpoint in the workspace.  
- **Model name:** `--model-name` (default `bank_fraud_predict`) is the Unity Catalog model name; full name is `catalog.schema.model_name`.  
- **Version:** Omit `--model-version` to use the version with alias `production`, or if none, the latest version.

Run `./deploy.sh --help` for all options.  

**Undeploy:** Run `undeploy.sh` to trash the dashboard, any Genie space, and the model serving endpoint created by deploy (and optionally remove the workspace path with `--remove-workspace`). It does not unregister ML models or delete experiments.

```bash
./undeploy.sh -p myprofile
./undeploy.sh -p myprofile --remove-workspace
```

The dashboard template lives in `dashboards/Retail_Bank_Fraud_Dashboard.lvdash.json`; a copy remains in `sql/` for reference.

#### Testing the Model Serving Endpoint.

Rest API JSON Payload - predicts 1 (Fraud)

```
{
  "dataframe_records": [
    {
      "countryOrig_name": "Turkey",
      "diffDest": 400000,
      "diffOrig": -400000,
      "oldBalanceDest": 0,
      "amount": 400000,
      "type": "TRANSFER",
      "newBalanceDest": 400000,
      "countryDest_name": "Canada"
    }
  ]
}
```
Rest API JSON Payload - predicts 0 ( Not Fraud)

```
{
  "dataframe_records": [
    {
      "countryOrig_name": "Russian Federation",
      "diffDest": 0,
      "diffOrig": 358291.03,
      "oldBalanceDest": 1278730.92,
      "amount": 358291.03,
      "type": "CASH_IN",
      "newBalanceDest": 1278730.92,
      "countryDest_name": "Russian Federation"
    }
  ]
}
```

## 3. Deploy UC Function + AI Chat Agent using UC Function tool

+ Create the UC function `explain_transaction_risk()`  
+ Register the UC model **`fraud_model_explain`** (LangChain + `ChatDatabricks` + foundation LLM)  
+ Serve it as a model serving endpoint (default name via deploy: **`bank-fraud-explain`** — hyphens; UC model uses underscores)

**Automated run** (assumes agent notebooks are already in the workspace, or omit `--skip-notebooks` to import everything first):

```bash
./deploy.sh -y -p myprofile -c my_catalog -s my_schema \
  --workspace-path /Users/you@example.com/dbx-bank-fraud \
  --skip-notebooks --skip-ml --skip-dashboard \
  --deploy-agent
```

** NOTE ** allow up to 20 minutes after the register and deploy agent has completed.  It takes a while for the serving endpoint to come on-line.   

Optional: `--llm-endpoint <name>` (default `databricks-gpt-5-2`), `--agent-serving-endpoint-name <name>` (default `bank-fraud-explain`), `--agent-register-timeout <seconds>` (default `5400` for the register/deploy notebook). Uses serverless compute unless you pass `--cluster-id`.

Test the agent to explain Fraud using the AI Playground.  Try transaction ID 3402687 as an example.

![Dashboard](./doc/fraud_agent_explain_example.png)


## 4. Databricks Apps Serving - deploy transaction simulator and web-app to App

Pre-reqs:
1. Lakebase Database: create a lakebase database in the workspace. Note the endpoint hostname and endpoint details (use the Lakebase "Connect" dialog to find the details required for connecting)
  Also use `databricks postgres list-projects -p <my-dbx-cli-profile>` to get the project ID


#### Smoke tests for Lakebase 
Check the Lakebase services are there and connectivity is set up correctly in `/app/.env`
- `python scripts/lakebase_smoke_test.py`

#### Deployment Steps - Databricks CLI

1. Ensure frontend is built:
```
cd <repo-dir>/app/frontend && npm run build 
```

2. Create the app (first time only):
```
databricks apps create fraud-analytics --profile <my-profile>
```

3. Sync app files to workspace:
```
databricks sync <repo-dir>/app /Workspace/Users/<user.name>@databricks.com/fraud-analytics --profile <my-profile>
```

4. Upload the built frontend separately (since .gitignore excludes dist/):
```
databricks workspace import-dir <repo-dir>/app/frontend/dist /Workspace/Users/<user.name>@databricks.com/fraud-analytics/frontend/dist --profile <my-profile> --overwrite
```

5. Deploy

Note that the `app/app.yaml` configuration binds the app to the Lakebase DB.  Check that this links up with the target lakebase DB.
```
databricks apps deploy fraud-analytics --source-code-path /Workspace/Users/<user.name>@databricks.com/fraud-analytics --profile <my-profile>
```

6. Check status and get the app URL:
```
databricks apps get fraud-analytics --profile <my-profile>
```

7. Get the Service Principal Name for the app:
```
databricks apps get fraud-analytics --profile <my-profile> |grep service_principal
```
(Alternatively Compute → Apps → <app> → identity in the Workspace UI)

8. Grant permissions for the App Service Principal to Query the `bank-fraud-predict` endpoint
Databricks Workspace UI -> Serving -> Permissions -> Add <service_principal_client_id> with "Can Query" privs

9. Restart the app (Re-Deploy)

10. Subsequent changes to code:
Updates after code change - after changing backend/frontend, repeat build → sync → import-dir dist → deploy (or at least sync + dist when only UI changes).

#### Post-deploy Lakebase bootstrapping

Declaring `resources:` in `app/app.yaml` is **not enough** on its own to make
the deployed app talk to Lakebase. The Databricks Apps platform only injects
`PGHOST / PGPORT / PGDATABASE / PGUSER` (and sometimes `PGPASSWORD`) once the
resource is **bound to the app in the UI**, and the app's **service principal**
(SP) still has to be granted Postgres-level privileges separately from any
Databricks-level ACLs.

Work through this checklist the first time you deploy to a new workspace. The
same steps apply after restoring or recreating the Lakebase instance.

1. **Bind the Lakebase + Serving resources in the App UI.**
   Compute → Apps → `fraud-analytics` → **Settings → Resources → Add resource**:

   - **Database (Lakebase)**
     - Resource key: `lakebase-db` ← must match the `name:` in `app.yaml`
     - Database instance: your Lakebase instance
     - Database name: `databricks_postgres`
     - Permission: **Read and write**
   - **Serving endpoint**
     - Resource key: `fraud-model`
     - Endpoint: `bank-fraud-predict`
     - Permission: **Can query**

   After saving, the app restarts and the **Environment** tab should show
   `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`. If these never appear, the
   binding did not take — fix that before anything else.

2. **Set `LAKEBASE_ENDPOINT`.** The platform injects host/user/db but not
   necessarily `PGPASSWORD`. The app falls back to OAuth token generation via
   the Databricks SDK, which needs the Lakebase endpoint path. It is already
   declared in `app/app.yaml`:

   ```yaml
   env:
     - name: LAKEBASE_ENDPOINT
       value: "projects/<project>/branches/<branch>/endpoints/<endpoint>"
   ```

   Update the value to match your workspace, or set it via the Environment tab.

3. **Pin a recent `databricks-sdk`.** Lakebase OAuth uses
   `WorkspaceClient.postgres.generate_database_credential(...)`, which only
   exists in recent SDK releases. `app/requirements.txt` pins
   `databricks-sdk>=0.96`; keep that, or bump it.

4. **Grant Postgres privileges to the app SP.** "Can manage" on the Lakebase
   instance is a *Databricks-level* ACL — Postgres inside the database needs
   its own grants. Connect as your own user (the smoke test or `psql` with an
   OAuth token will do) and run:

   ```sql
   -- Replace with the SP UUID from PGUSER (also = DATABRICKS_CLIENT_ID in the
   -- app Environment tab). Keep the double quotes.
   GRANT USAGE, CREATE ON SCHEMA public TO "<APP_SP_UUID>";
   GRANT SELECT, INSERT, UPDATE, DELETE ON scored_transactions TO "<APP_SP_UUID>";
   ALTER DEFAULT PRIVILEGES IN SCHEMA public
     GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO "<APP_SP_UUID>";
   ```

5. **Make the app SP own its own table** (first deploy, or when reusing an
   existing Lakebase that already contains `scored_transactions`).

   The app's startup runs `CREATE TABLE IF NOT EXISTS` *and*
   `CREATE INDEX IF NOT EXISTS`. Postgres enforces table ownership on the
   index DDL even when the indexes already exist, so if your user created the
   table earlier (e.g. during local dev) the deployed app will fail with
   `must be owner of table scored_transactions`.

   The cleanest fix is to drop the table and let the SP recreate it on
   startup. The rows are already replicated to Delta via Lakehouse Sync, so
   there is no real data loss for the demo:

   ```sql
   DROP TABLE IF EXISTS scored_transactions CASCADE;
   ```

   Restart the app. It creates the table, becomes the owner, and subsequent
   restarts are idempotent.

   If you need to preserve the Lakebase rows, back them up first, let the SP
   recreate the table, then reload:

   ```sql
   CREATE TABLE scored_transactions_backup AS SELECT * FROM scored_transactions;
   DROP TABLE scored_transactions CASCADE;
   -- restart the app so the SP creates the new table
   INSERT INTO scored_transactions SELECT * FROM scored_transactions_backup;
   DROP TABLE scored_transactions_backup;
   ```

   (Transferring ownership via `ALTER TABLE ... OWNER TO "<APP_SP_UUID>"`
   usually fails in Lakebase with `must be able to SET ROLE` because your user
   is not automatically a member of the auto-provisioned SP role — drop and
   recreate is less ceremony.)

6. **Verify.** Restart the app and check `/logz`:

   - `OAuth token generated, expires at ...`
   - **no** `Using in-memory MockDB` line
   - bridge inserts appearing as transactions score

   You can also run `python scripts/lakebase_smoke_test.py` from your laptop
   to confirm the table is present and growing.

# OLD #
  
----------------------------------------------------------------------------------------------------
# Manual Setup Notes - Base Data, Tables and Views

File-based data from `./data` folder in this repo needs to be loaded to a Unity Catalog Volume.  This can be a UC *Managed Volume* (storage and setup managed within Databricks) or an *External Volume* (files stored in an external cloud storage bucket mapped to this volume).  

## Copy the CSV and JSON data to a Unity Catalog Volume

1. Identify the Databricks Catalog, Schema, Volume name to load the data to.
2. export local env vars:

- `export UNITY_CATALOG=<catalog>`
- `export UNITY_SCHEMA=<schema>`
- `export UNITY_VOLUME=<volume>`

Manually, in the Workspace GUI, create the volume directory structure:

```
<volume>  
   ├── retail/
        ├── transactions/
        │── fraud_reports/ 
        ├── customers/
        ├── country_code/            
```

From the root of this repo:

```
databricks fs cp ./data/transactions/ dbfs:/Volumes/$UNITY_CATALOG/$UNITY_SCHEMA/$UNITY_VOLUME/retail/transactions/ --overwrite --recursive

databricks fs cp ./data/fraud_reports/ dbfs:/Volumes/$UNITY_CATALOG/$UNITY_SCHEMA/$UNITY_VOLUME/retail/fraud_reports/ --overwrite --recursive  

databricks fs cp ./data/customers_json/ dbfs:/Volumes/$UNITY_CATALOG/$UNITY_SCHEMA/$UNITY_VOLUME/retail/customers/ --overwrite --recursive

databricks fs cp ./data/country_coordinates/country_coordinates.csv dbfs:/Volumes/$UNITY_CATALOG/$UNITY_SCHEMA/$UNITY_VOLUME/retail/country_code/ --overwrite
```

## Create Base Tables and populate.

Before running the python scripts, make sure to set the Databricks SDK profile to work in.  EG

```
export DATABRICKS_CONFIG_PROFILE=my_profile_name
```

These tables can be setup from a remote command prompt session, running in the root of this repo.  Use the `etl/create.py` script to create the tables.

Connect to a remote workspace as per the instructions above in *IDE Connect to the workspace environment*

1. Create Bronze transactions `bronze_transactions`

- The `create.py` script runs differently for the `bronze_transactions` table and runs an Autoloader incremental load (not a simple CTAS)

Specify the catalog `-c` the schema `-s` to work in.  
Specify the volume `-v` in which the source data is staged in.   

Example:  

```
python etl/create.py -c $UNITY_CATALOG -s $UNITY_SCHEMA -t bronze_transactions -v bank-fraud
```

1. Create the `fraud_reports`, `banking_customers`, `country_coordinates` tables

- If the source folder name is different from the table name, specify the folder with the `-f` option.

```
python etl/create.py -c $UNITY_CATALOG -s $UNITY_SCHEMA -t fraud_reports -v bank-fraud 
```

 Note the option `--format json` for JSON format data.  

```
python etl/create.py -c $UNITY_CATALOG -s $UNITY_SCHEMA -t banking_customers -v bank-fraud -f customers --format json
```

```
python etl/create.py -c $UNITY_CATALOG -s $UNITY_SCHEMA -t country_coordinates -v bank-fraud -f country_code 
```

1. Run the Silver Transactions Merge-Load to Create the `silver_transactions` table

```
python etl/create.py -c $UNITY_CATALOG -s $UNITY_SCHEMA -t silver_transactions -v bank-fraud
```

1. Create the Gold Transactions View

```
python etl/create.py -c $UNITY_CATALOG -s $UNITY_SCHEMA -t gold_transactions -v bank-fraud
```

## Cleardown Base Tables and Views

1. Drop Gold Transactions View:

```
python etl/destroy.py -c $UNITY_CATALOG -s $UNITY_SCHEMA -t gold_transactions
```

1. Drop Silver Transactions Table:

```
python etl/destroy.py -c $UNITY_CATALOG -s $UNITY_SCHEMA -t silver_transactions
```

1. Drop Bronze Transactions, Fraud Reports, Banking Customers, Country Coordinates.

```
python etl/destroy.py -c $UNITY_CATALOG -s $UNITY_SCHEMA -t bronze_transactions
python etl/destroy.py -c $UNITY_CATALOG -s $UNITY_SCHEMA -t fraud_reports
python etl/destroy.py -c $UNITY_CATALOG -s $UNITY_SCHEMA -t banking_customers
python etl/destroy.py -c $UNITY_CATALOG -s $UNITY_SCHEMA -t country_coordinates
```

1. Delete all the files stored in Unity Catalog Volume(s)

```
databricks fs rm dbfs:/Volumes/$UNITY_CATALOG/$UNITY_SCHEMA/$UNITY_VOLUME/retail/transactions -r

databricks fs rm dbfs:/Volumes/$UNITY_CATALOG/$UNITY_SCHEMA/$UNITY_VOLUME/retail/fraud_reports -r

databricks fs rm dbfs:/Volumes/$UNITY_CATALOG/$UNITY_SCHEMA/$UNITY_VOLUME/retail/customers -r

databricks fs rm dbfs:/Volumes/$UNITY_CATALOG/$UNITY_SCHEMA/$UNITY_VOLUME/retail/country_code -r
```

# Setup - ML Models and AI Agents

## Machine Learning and MLflow

1. Train a model with multiple training runs in an MLflow experiment using the `./notebooks/fraud_model_training.py` notebook
  set the Unity Catalog *Catalog* and *Schema* values where prompted in the notebook
2. Deploy the best performing model from the MLFlow experiments to Unity Catalog using the  `./notebooks/fraud_model_deploy.py` notebook.
3. Run a batch set of predictions using the model registered in Unity Catalog and store the results using the `./notebooks/fraud_model_run.py` notebook

# Job Configuration

Use the two notebooks 

- `./jobs/run_transaction_file_load.py`
- `./jobs/run_silver_table_load.py`

to call the ETL code in `./etl` and run the ETL pipeline.  These read the parameters configured in the Databricks Job run-time configuration and pass them to the code execution.