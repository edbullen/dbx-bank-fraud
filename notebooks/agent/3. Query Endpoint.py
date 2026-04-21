# Databricks notebook source
# MAGIC %md
# MAGIC # Query the deployed fraud agent (Model Serving)
# MAGIC
# MAGIC Call the agent’s **invocations** URL with chat-style `messages`.
# MAGIC
# MAGIC **URL:** The **Unity Catalog model** is `…fraud_model_explain` (underscores). The **serving endpoint name** is separate (e.g. `bank-fraud-explain` with hyphens), set in Notebook 2.
# MAGIC
# MAGIC Or paste **Invocations URL** from Notebook 2, or leave both empty to auto-build the default `agents_*` / `served-models/...` path from catalog, schema, and version.

# COMMAND ----------

# DBTITLE 1,Widgets
# Optional: full URL from Notebook 2 after deploy — most reliable if naming ever differs.
dbutils.widgets.text(
    "invocations_url",
    "",
    "Invocations URL (override; empty = build from catalog/schema)",
)
dbutils.widgets.text("unity_catalog", "default_catalog", "Unity Catalog")
dbutils.widgets.text("unity_schema", "default_schema", "Unity Schema")
dbutils.widgets.text(
    "agent_model_version",
    "latest",
    "Model version: latest or e.g. 8 (default agents_* path only)",
)
dbutils.widgets.text(
    "serving_endpoint_name",
    "",
    "Named endpoint from Notebook 2, exact string (e.g. bank-fraud-explain); empty = agents_* path",
)

invocations_url_override = (dbutils.widgets.get("invocations_url") or "").strip()
unity_catalog = dbutils.widgets.get("unity_catalog")
unity_schema = dbutils.widgets.get("unity_schema")
agent_version_widget = (dbutils.widgets.get("agent_model_version") or "latest").strip().lower()
serving_endpoint_name = (dbutils.widgets.get("serving_endpoint_name") or "").strip()

UC_MODEL_NAME = f"{unity_catalog}.{unity_schema}.fraud_model_explain"

# COMMAND ----------

# DBTITLE 1,Resolve invocations URL
from urllib.parse import urlparse

# Same UC model as Notebook 2: fraud_model_explain.


def _workspace_base_url():
    """Databricks workspace origin (e.g. https://adb-….azuredatabricks.net)."""
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    api = (ctx.apiUrl().get() or "").rstrip("/")
    if not api:
        raise RuntimeError("Could not read apiUrl from notebook context.")
    parsed = urlparse(api)
    if parsed.scheme and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}".rstrip("/")
    return api


def _org_query_suffix():
    try:
        wid = (
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .workspaceId()
            .get()
        )
        if wid:
            return f"?o={wid}"
    except Exception:
        pass
    return ""


def build_named_endpoint_invocations_url(endpoint_name: str) -> str:
    """Notebook 2 with serving_endpoint_name set: .../serving-endpoints/<name>/invocations"""
    base = _workspace_base_url()
    return f"{base}/serving-endpoints/{endpoint_name}/invocations{_org_query_suffix()}"


def build_agent_invocations_url(catalog: str, schema: str, model_version: str) -> str:
    """
    Default agents.deploy URL (no custom endpoint_name):
      {workspace}/serving-endpoints/agents_{slug}/served-models/{slug}_{version}/invocations[?o=...]
    """
    base = _workspace_base_url()
    slug = f"{catalog}.{schema}.fraud_model_explain".replace(".", "-")
    endpoint_name = f"agents_{slug}"
    served_name = f"{slug}_{model_version}"
    return (
        f"{base}/serving-endpoints/{endpoint_name}/served-models/{served_name}/invocations"
        f"{_org_query_suffix()}"
    )


if invocations_url_override:
    endpoint = invocations_url_override
    print("Using **invocations_url** widget (full URL override).")
elif serving_endpoint_name:
    endpoint = build_named_endpoint_invocations_url(serving_endpoint_name)
    print(f"Using **serving_endpoint_name** = `{serving_endpoint_name}` (named deploy path).")
else:
    if agent_version_widget == "latest":
        from mlflow.tracking import MlflowClient

        safe_name = UC_MODEL_NAME.replace("'", "''")
        client = MlflowClient(registry_uri="databricks-uc")
        versions = client.search_model_versions(f"name='{safe_name}'")
        if not versions:
            raise RuntimeError(
                f"No UC model versions for {UC_MODEL_NAME}. Run Notebook 2, or set **invocations_url** "
                "to `deployment.query_endpoint`."
            )
        ver = str(max(int(v.version) for v in versions))
    else:
        ver = agent_version_widget
    endpoint = build_agent_invocations_url(unity_catalog, unity_schema, ver)
    print(f"Built URL from workspace + serving path (UC model version **{ver}**).")
    print(
        "If calls fail (404), the deployed served model version may differ from latest registered; "
        "set **agent_model_version** to the deployed version or paste **invocations_url** from Notebook 2."
    )

print(endpoint)

# COMMAND ----------

# DBTITLE 1,Query helpers
import json

import requests


def _get_host_and_token():
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    return ctx.apiUrl().get(), ctx.apiToken().get()


def query_serving_endpoint(endpoint_url: str, messages):
    """
    POST to a Databricks Model Serving /invocations endpoint.

    Args:
      endpoint_url: full invocations URL (built above or override widget).
      messages: list of {"role": "...", "content": "..."}.

    Returns:
      dict with payload_used and response.
    """
    _, token = _get_host_and_token()

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    payloads = [
        {"dataframe_records": [{"messages": messages}]},
        {"instances": [{"messages": messages}]},
    ]

    last = None
    for payload in payloads:
        resp = requests.post(
            endpoint_url, headers=headers, data=json.dumps(payload), timeout=120
        )
        try:
            body = resp.json()
        except Exception:
            body = resp.text

        if resp.status_code == 200:
            return {"payload_used": payload, "response": body}
        last = {"status_code": resp.status_code, "body": body, "payload": payload}

    raise RuntimeError(
        f"Invocation failed. Last error:\n{json.dumps(last, indent=2)[:4000]}"
    )


def extract_text(serving_response):
    """Best-effort string / chat payload from MLflow-style serving responses."""
    r = serving_response["response"]

    if isinstance(r, dict) and "predictions" in r:
        preds = r["predictions"]
        if isinstance(preds, list) and preds:
            p0 = preds[0]
            if isinstance(p0, dict) and p0.get("object") == "chat.completion":
                ch = p0.get("choices") or []
                if ch and isinstance(ch[0], dict):
                    msg = ch[0].get("message") or {}
                    return msg.get("content", p0)
            if isinstance(p0, str):
                return p0
            return p0
        return preds

    return r


# COMMAND ----------

# DBTITLE 1,Example query (historical, explicit tag)
messages = [
    {"role": "user", "content": "Why does historical txn 620650 look risky?"},
]

result = query_serving_endpoint(endpoint, messages)
print(
    "Payload used:",
    "dataframe_records" if "dataframe_records" in result["payload_used"] else "instances",
)
print("Raw response:", result["response"])

text = extract_text(result)
print("\n--- Extracted text / assistant payload ---\n")
print(text)

# COMMAND ----------

# DBTITLE 1,Example query (live, session-context prompt)
# Substitute a real id from live_transactions (e.g. one flagged as fraud in the
# app UI). The first message sets the dataset context for the rest of the chat.
live_messages = [
    {"role": "user", "content": "We are working off live transactions for this session."},
    {"role": "user", "content": "Why does transaction 10000000 look risky?"},
]

result_live = query_serving_endpoint(endpoint, live_messages)
print("\n--- Live (session-context) ---\n")
print(extract_text(result_live))

# COMMAND ----------

# DBTITLE 1,Example query (live, explicit tag)
# Most reliable form: the explicit "live txn" tag forces the live tool
# regardless of session context.
live_tag_messages = [
    {"role": "user", "content": "Why does live txn 10000000 look risky?"},
]

result_live_tag = query_serving_endpoint(endpoint, live_tag_messages)
print("\n--- Live (explicit tag) ---\n")
print(extract_text(result_live_tag))

# COMMAND ----------

# DBTITLE 1,Chat-style access (if response is OpenAI-shaped)
if isinstance(text, dict) and text.get("choices"):
    print(text["choices"][0].get("message"))
