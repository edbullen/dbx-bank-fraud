# Databricks notebook source
# MAGIC %md
# MAGIC # Register an Agent and Serve it
# MAGIC + Create a custom LLM based on a Foundation Model with custom prompt and tools
# MAGIC + Log the Model in MLflow so that it can be managed in Unity Catalog
# MAGIC + Serve the model as a Model Serving endpoint (needs to be in UC to do this)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 - libraries install and imports
# MAGIC
# MAGIC Developed with `databricks-langchain`, `langchain`, `mlflow`, `databricks-agents`.

# COMMAND ----------

# MAGIC %pip install -U -qqqq databricks-langchain langchain langchain-core mlflow databricks-agents
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Configure widgets (catalog, schema, LLM endpoint, optional serving name)
dbutils.widgets.text("unity_catalog", "default_catalog", "Unity Catalog")
dbutils.widgets.text("unity_schema", "default_schema", "Unity Schema")
dbutils.widgets.text("llm_endpoint", "databricks-gpt-5-2", "LLM endpoint name (ChatDatabricks)")
dbutils.widgets.text(
    "serving_endpoint_name",
    "",
    "Serving endpoint name (optional; hyphens OK, e.g. bank-fraud-explain — separate from UC model underscores)",
)

unity_catalog = dbutils.widgets.get("unity_catalog")
unity_schema = dbutils.widgets.get("unity_schema")
LLM_ENDPOINT = dbutils.widgets.get("llm_endpoint")
SERVING_ENDPOINT_NAME = (dbutils.widgets.get("serving_endpoint_name") or "").strip()

# UC dotted names for toolkit / resources (no backticks)
UC_FUNCTION_HISTORICAL = f"{unity_catalog}.{unity_schema}.explain_transaction_risk"
UC_FUNCTION_LIVE = f"{unity_catalog}.{unity_schema}.explain_live_transaction_risk"
UC_FUNCTION_FQNS = [UC_FUNCTION_HISTORICAL, UC_FUNCTION_LIVE]
UC_MODEL_NAME = f"{unity_catalog}.{unity_schema}.fraud_model_explain"
UC_GOLD_TABLE = f"{unity_catalog}.{unity_schema}.gold_transactions"
UC_LIVE_TABLE = f"{unity_catalog}.{unity_schema}.live_transactions"

print(f"Catalog: {unity_catalog}, Schema: {unity_schema}")
print(f"LLM endpoint: {LLM_ENDPOINT}")
print(f"UC functions: {UC_FUNCTION_FQNS}, Model: {UC_MODEL_NAME}")
if SERVING_ENDPOINT_NAME:
    print(f"Serving endpoint name: {SERVING_ENDPOINT_NAME}")
else:
    print("Serving endpoint name: (default — auto-generated agents_* path)")

# COMMAND ----------

# MAGIC %pip show langchain
# MAGIC %pip show langchain-core
# MAGIC %pip show databricks-langchain

# COMMAND ----------

# DBTITLE 1,Imports
import langchain
import langchain_core

from langchain.agents import create_agent

from databricks_langchain import ChatDatabricks
from databricks_langchain.uc_ai import UCFunctionToolkit

# COMMAND ----------

# DBTITLE 1,Debug versions
print("langchain:", langchain.__version__)
print("langchain_core:", langchain_core.__version__)

import langchain.agents as agents
print("langchain.agents exports:", [x for x in dir(agents) if "Agent" in x])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2 - create an agent with tools

# COMMAND ----------

# DBTITLE 1,Create agent
llm = ChatDatabricks(
    endpoint=LLM_ENDPOINT,
    temperature=0.1,
    max_tokens=500,
)

toolkit = UCFunctionToolkit(function_names=UC_FUNCTION_FQNS)
tools = toolkit.tools

SYSTEM_PROMPT = (
    "You are a retail banking fraud-risk assistant. Two tools are available:\n"
    f"- `{UC_FUNCTION_LIVE}`: explains a LIVE transaction by id (real-time data "
    "flowing through the deployed app, sourced from `live_transactions`).\n"
    f"- `{UC_FUNCTION_HISTORICAL}`: explains a HISTORICAL transaction by id "
    "(static training data, sourced from `gold_transactions`).\n\n"
    "Routing rules:\n"
    "1. If the user explicitly tags an id (e.g. 'live txn N' or 'historical "
    "txn N'), honour the tag.\n"
    "2. If the user has set a session context (e.g. 'we are working off live "
    "transactions' or 'use historical'), apply it to subsequent bare ids.\n"
    "3. If unspecified, default to LIVE and tell the user which dataset you "
    "queried.\n\n"
    "The tool returns a table with a single row. Use the first row. Explain "
    "the top 2-3 signals clearly and briefly, and include the risk score."
)

agent = create_agent(
    model=llm,
    tools=tools,
    system_prompt=SYSTEM_PROMPT,
)

# COMMAND ----------

# DBTITLE 1,Test the agent (historical id, explicit tag)
from pprint import pprint

historical_transaction_id = 620650

result = agent.invoke({
    "messages": [
        {"role": "user", "content": f"Why does historical txn {historical_transaction_id} look risky?"}
    ]
})

pprint(result["messages"][-1])

# COMMAND ----------

# DBTITLE 1,Test the agent (live id via session context)
# Substitute a real id from live_transactions if you have one running.
live_transaction_id = 10000000

result_live = agent.invoke({
    "messages": [
        {"role": "user", "content": "We are working off live transactions for this session."},
        {"role": "user", "content": f"Why does transaction {live_transaction_id} look risky?"}
    ]
})

pprint(result_live["messages"][-1])

# COMMAND ----------

# DBTITLE 1,Cleaned text output (last test)
final_msg = result_live["messages"][-1]
print(getattr(final_msg, "content", final_msg))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3 - Log the model in MLflow and register in Unity Catalog

# COMMAND ----------

# DBTITLE 1,Check MLflow / LangChain versions
import mlflow as _mlf
print("mlflow:", _mlf.__version__)
print("langchain:", langchain.__version__)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3.1 - Write a code-based model file (fraud_model_explain.py)

# COMMAND ----------

# DBTITLE 1,Create model code file
local_code_path = "/tmp/fraud_model_explain.py"

# Embedded served code: placeholders avoid brace-escaping in f-strings
_AGENT_BODY = r'''
import mlflow
from databricks_langchain import ChatDatabricks
from databricks_langchain.uc_ai import UCFunctionToolkit
from langchain.agents import create_agent
from langchain_core.runnables import RunnableLambda

LLM_ENDPOINT = "__LLM_ENDPOINT__"

UC_FUNCTION_HISTORICAL = "__UC_FUNCTION_HISTORICAL__"
UC_FUNCTION_LIVE = "__UC_FUNCTION_LIVE__"
UC_FUNCTION_FQNS = [UC_FUNCTION_HISTORICAL, UC_FUNCTION_LIVE]

SYSTEM_PROMPT = (
    "You are a retail banking fraud-risk assistant. Two tools are available: "
    "`" + UC_FUNCTION_LIVE + "` explains a LIVE transaction by id (real-time data "
    "flowing through the deployed app, sourced from live_transactions). "
    "`" + UC_FUNCTION_HISTORICAL + "` explains a HISTORICAL transaction by id "
    "(static training data, sourced from gold_transactions). "
    "Routing rules: "
    "1) If the user explicitly tags an id ('live txn N' or 'historical txn N'), honour the tag. "
    "2) If the user has set a session context ('we are working off live transactions' / 'use historical'), "
    "apply it to subsequent bare ids. "
    "3) If unspecified, default to LIVE and tell the user which dataset you queried. "
    "The tool returns a table with one row; use the first row. "
    "Explain the top 2-3 signals clearly and briefly, and include the risk score."
)

llm = ChatDatabricks(endpoint=LLM_ENDPOINT, temperature=0.1, max_tokens=500)
toolkit = UCFunctionToolkit(function_names=UC_FUNCTION_FQNS)
tools = toolkit.tools
_agent = create_agent(model=llm, tools=tools, system_prompt=SYSTEM_PROMPT)

def _to_text(msg):
    if isinstance(msg, dict):
        return msg.get("content", "")
    return getattr(msg, "content", str(msg))

def _msg_obj_to_dict(m):
    role = getattr(m, "type", None) or getattr(m, "role", None) or m.__class__.__name__.lower()
    if role == "human":
        role = "user"
    elif role == "ai":
        role = "assistant"
    content = getattr(m, "content", None)
    return {"role": role, "content": content if content is not None else str(m)}

def _normalise_messages(x):
    if isinstance(x, list):
        if len(x) == 1 and isinstance(x[0], list):
            x = x[0]
        if len(x) == 0:
            return [{"role": "user", "content": ""}]
        if isinstance(x[0], dict):
            return x
        return [_msg_obj_to_dict(m) for m in x]
    if isinstance(x, dict) and "messages" in x:
        msgs = x["messages"]
        if isinstance(msgs, list) and len(msgs) == 1 and isinstance(msgs[0], list):
            msgs = msgs[0]
        return _normalise_messages(msgs)
    if isinstance(x, str):
        return [{"role": "user", "content": x}]
    return [{"role": "user", "content": str(x)}]

def _predict(inputs):
    msgs = _normalise_messages(inputs)
    state = _agent.invoke({"messages": msgs})
    final_msg = state["messages"][-1]
    return _to_text(final_msg)

model = RunnableLambda(_predict)
mlflow.models.set_model(model)
'''

agent_py = (
    _AGENT_BODY.replace("__LLM_ENDPOINT__", LLM_ENDPOINT.replace("\\", "\\\\").replace('"', '\\"'))
    .replace("__UC_FUNCTION_HISTORICAL__", UC_FUNCTION_HISTORICAL)
    .replace("__UC_FUNCTION_LIVE__", UC_FUNCTION_LIVE)
)

# COMMAND ----------

# DBTITLE 1,Write to tmp file
with open(local_code_path, "w", encoding="utf-8") as f:
    f.write(agent_py)

print("Updated:", local_code_path)

# COMMAND ----------

# DBTITLE 1,Check the wrapper format
import importlib.util

spec = importlib.util.spec_from_file_location("fraud_model_explain", local_code_path)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

nested = {"messages": [[{"role": "user", "content": "Say hello."}]]}
print(mod.model.invoke(nested)[:120])

normal = {"messages": [{"role": "user", "content": "Say hello."}]}
print(mod.model.invoke(normal)[:120])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3.2 - Log the code model and register in Unity Catalog

# COMMAND ----------

# DBTITLE 1,MLflow resource classes
import mlflow
print("mlflow:", mlflow.__version__)

from mlflow.models import resources as r
print([name for name in dir(r) if "Resource" in name or "Databricks" in name or "Unity" in name or "Endpoint" in name or "Function" in name])

# COMMAND ----------

# DBTITLE 1,Register model
from mlflow.models.signature import infer_signature

mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")

resources = [
    r.DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT),
    r.DatabricksFunction(function_name=UC_FUNCTION_HISTORICAL),
    r.DatabricksFunction(function_name=UC_FUNCTION_LIVE),
    r.DatabricksTable(table_name=UC_GOLD_TABLE),
    r.DatabricksTable(table_name=UC_LIVE_TABLE),
]

input_example = {"messages": [{"role": "user", "content": "Say hello then explain why transaction 123 looks risky."}]}
output_example = "Hello. Risk score 0/100. Key factors: ..."

signature = infer_signature(input_example, output_example)

with mlflow.start_run() as run:
    logged = mlflow.langchain.log_model(
        lc_model=local_code_path,
        name="agent",
        input_example=input_example,
        signature=signature,
        resources=resources,
    )
    model_uri = logged.model_uri

mv = mlflow.register_model(model_uri=model_uri, name=UC_MODEL_NAME)

print("Registered:", mv.name, "version", mv.version)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3.3 - Deploy the Agent and Serve it
# MAGIC
# MAGIC **Permission requirements:** EXECUTE on both UC functions (`explain_transaction_risk`, `explain_live_transaction_risk`), SELECT on `gold_transactions` and `live_transactions`, permission to call the LLM serving endpoint.

# COMMAND ----------

# DBTITLE 1,Deploy
from databricks import agents

_deploy_kw = {}
if SERVING_ENDPOINT_NAME:
    _deploy_kw["endpoint_name"] = SERVING_ENDPOINT_NAME

deployment = agents.deploy(UC_MODEL_NAME, str(mv.version), **_deploy_kw)
print(f"\n\nAgent endpoint for version {mv.version}:", deployment.query_endpoint)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deployment URL
# MAGIC - **Named endpoint** (widget *Serving endpoint name* set): `{workspace_base}/serving-endpoints/<name>/invocations`
# MAGIC - **Default** (empty name): auto `agents_*` path — copy `deployment.query_endpoint` or use Notebook 3’s catalog/schema URL builder.

# COMMAND ----------

# DBTITLE 1,Store the endpoint URL
endpoint = deployment.query_endpoint
print(endpoint)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Test the endpoint
# MAGIC It might take ~10 minutes for a new endpoint to deploy. Then use Notebook 3 or the model playground to test.
