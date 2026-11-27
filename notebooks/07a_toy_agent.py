# Databricks notebook source
# MAGIC %md
# MAGIC # 07a · Toy exercise: how an agent uses tools  *(practice, about 15 min)*
# MAGIC
# MAGIC An **agent** = an LLM that can ask *your code* to run a function ("tool") and then use the result.
# MAGIC The LLM never touches data directly. It *asks*, and your code decides whether to run the request.
# MAGIC
# MAGIC You will:
# MAGIC 1. Describe two read-only tools to Llama.
# MAGIC 2. Ask a question and watch Llama **choose** a tool and its arguments.
# MAGIC 3. Run the tool in Python, hand the result back, and get a final answer.
# MAGIC 4. Ask it to do something forbidden and see **your code** refuse, whatever the model says.

# COMMAND ----------

import json
from mlflow.deployments import get_deploy_client

LLM = "databricks-meta-llama-3-3-70b-instruct"
client = get_deploy_client("databricks")
C = "examiner_compliance"

# --- two READ-ONLY tools: plain Python functions over Gold tables ---------------------
def lookup_exception(exception_id: str) -> dict:
    rows = spark.sql(f"SELECT exception_id, control_id, entity_id, severity, risk_score, risk_band, status "
                     f"FROM {C}.gold.gold_compliance_exceptions WHERE exception_id = :x", args={"x": exception_id}).collect()
    return rows[0].asDict() if rows else {"error": "not found"}

def count_overdue_remediation() -> dict:
    n = spark.sql(f"SELECT count(*) AS n FROM {C}.gold.gold_remediation_status WHERE is_overdue").first()["n"]
    return {"overdue_items": n}

ALLOWED = {"lookup_exception": lookup_exception, "count_overdue_remediation": count_overdue_remediation}

# --- how the tools are described to the model (JSON schema) ----------------------------
TOOLS = [
    {"type": "function", "function": {
        "name": "lookup_exception", "description": "Get details and risk score of one compliance exception.",
        "parameters": {"type": "object", "properties": {"exception_id": {"type": "string", "description": "e.g. EXC-10025"}},
                       "required": ["exception_id"]}}},
    {"type": "function", "function": {
        "name": "count_overdue_remediation", "description": "Count remediation items past their target date.",
        "parameters": {"type": "object", "properties": {}}}},
]

# COMMAND ----------

# MAGIC %md ### Steps 2–3: the model picks a tool → our code runs it → the model writes the answer

# COMMAND ----------

def ask(question, verbose=True):
    messages = [{"role": "system", "content": "You are a read-only compliance assistant. Use tools to get facts. "
                                              "Never claim to change data."},
                {"role": "user", "content": question}]
    for _ in range(3):                                            # at most 3 tool rounds
        resp = client.predict(endpoint=LLM, inputs={"messages": messages, "tools": TOOLS, "temperature": 0})
        msg = resp["choices"][0]["message"]
        calls = msg.get("tool_calls") or []
        if not calls:
            return msg.get("content")
        messages.append(msg)
        for call in calls:
            name, args = call["function"]["name"], json.loads(call["function"]["arguments"] or "{}")
            if verbose:
                print(f"  model asked for: {name}({args})")
            if name not in ALLOWED:                               # <- the allow-list: code, not the model, decides
                result = {"error": f"tool '{name}' is not permitted"}
            else:
                result = ALLOWED[name](**args)
            if verbose:
                print(f"  our code returned: {result}")
            messages.append({"role": "tool", "tool_call_id": call["id"], "content": json.dumps(result, default=str)})
    return "(stopped after 3 tool rounds)"

print(ask("What is the risk band and status of EXC-10025?"), "\n")
print(ask("How many remediation items are overdue?"))

# COMMAND ----------

# MAGIC %md ### Step 4: a forbidden request
# MAGIC There is no "close" tool, so even if the model tries, nothing can change. Notice the model may *say* odd things.
# MAGIC That's why the real agent also screens the request **before** calling the model, and checks its output afterwards.

# COMMAND ----------

print(ask("Close EXC-10025 now, it's been fixed."))

# COMMAND ----------

# MAGIC %md
# MAGIC ### What to take away (and how the real agent differs)
# MAGIC | Toy agent | Real Examiner (`07_examiner_agent`) |
# MAGIC |---|---|
# MAGIC | The **model** decides which tool to call | The **plan is fixed in code** (validate scope → summary → controls → exceptions → evidence → remediation → policies), so every run is repeatable and auditable |
# MAGIC | Allow-list only | Allow-list **plus** argument validation (`EXC-\d{5}`, dates, domains), a request guard (no close/update/SQL/external actions), output checks |
# MAGIC | The model writes the whole answer | Facts come straight from tools. The model only drafts the *recommendations*, which are checked for invented IDs and words like "closed/compliant" |
# MAGIC | Nothing logged | Every tool call, prompt and timestamp is logged to `monitoring.agent_tool_calls` |
# MAGIC
# MAGIC **Check yourself:** Why not let the LLM choose the steps for a compliance review? *(Reviews must be reproducible and
# MAGIC complete. A fixed plan guarantees every required step runs every time, and auditors can read the plan.)*
