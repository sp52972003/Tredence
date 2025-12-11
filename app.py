
import asyncio
import uuid
import copy
from typing import Dict, Any, Optional
from fastapi import FastAPI
from pydantic import BaseModel
import persistence  # local db module

app = FastAPI(title="Mini Workflow Engine — Data Quality Pipeline (persistent)")

# -------------------------
# In-memory caches
# -------------------------
GRAPHS: Dict[str, Dict[str, Any]] = {}
RUNS: Dict[str, Dict[str, Any]] = {}

# -------------------------
# Tools
# -------------------------
def profile_data_tool(state: Dict[str, Any]) -> Dict[str, Any]:
    data = state.get("data", [])
    state["profile"] = {
        "rows": len(data),
        "nulls": sum(1 for v in data if v is None)
    }
    return state

def detect_anomalies_tool(state: Dict[str, Any]) -> Dict[str, Any]:
    data = state.get("data", [])
    low, high = state.get("anomaly_bounds", (0, 100))
    anomalies = [v for v in data if v is not None and (v < low or v > high)]
    state["anomalies"] = {"count": len(anomalies), "values": anomalies[:10]}
    return state

def generate_rules_tool(state: Dict[str, Any]) -> Dict[str, Any]:
    rules = []
    if state.get("profile", {}).get("nulls", 0) > 0:
        rules.append({"name": "fill_null", "action": "fill", "value": 0})
    if state.get("anomalies", {}).get("count", 0) > 0:
        low, high = state.get("anomaly_bounds", (0, 100))
        rules.append({"name": "clip", "action": "clip", "low": low, "high": high})
    state["rules"] = rules
    return state

def apply_rules_tool(state: Dict[str, Any]) -> Dict[str, Any]:
    data = state.get("data", [])
    rules = state.get("rules", [])
    new_data = []

    for v in data:
        if v is None:
            fill_rule = next((r for r in rules if r["name"] == "fill_null"), None)
            new_data.append(fill_rule["value"] if fill_rule else v)
            continue

        clip_rule = next((r for r in rules if r["name"] == "clip"), None)
        if clip_rule:
            low, high = clip_rule["low"], clip_rule["high"]
            new_data.append(max(low, min(high, v)))
        else:
            new_data.append(v)

    state["data"] = new_data
    return state

TOOLS = {
    "profile": profile_data_tool,
    "detect_anomalies": detect_anomalies_tool,
    "generate_rules": generate_rules_tool,
    "apply_rules": apply_rules_tool,
}

# -------------------------
# Graph model
# -------------------------
class GraphDef(BaseModel):
    nodes: Dict[str, str]
    edges: Dict[str, str]
    start_node: str
    loop_condition: Optional[Dict[str, Any]] = None

# -------------------------
# Helpers
# -------------------------
def _resolve_metric(state: Dict[str, Any], metric_path: str):
    cur = state
    for part in metric_path.split("."):
        if cur is None:
            return None
        cur = cur.get(part)
    return cur

def _persist_run(run_id: str):
    if run_id in RUNS:
        persistence.update_run(run_id, RUNS[run_id])

# -------------------------
# Execution Engine
# -------------------------
async def execute_graph(graph_id: str, run_id: str):
    graph = GRAPHS.get(graph_id) or persistence.load_graph(graph_id)
    if graph is None:
        RUNS[run_id]["status"] = "failed"
        RUNS[run_id]["log"].append("Graph not found during execution.")
        _persist_run(run_id)
        return

    GRAPHS[graph_id] = graph

    state = RUNS[run_id]["state"]
    log = RUNS[run_id]["log"]
    current = graph["start_node"]
    visited = 0

    prev_state = copy.deepcopy(state)

    def condition_satisfied():
        cond = graph.get("loop_condition")
        if not cond:
            return False
        metric_val = _resolve_metric(state, cond["metric"])
        if metric_val is None:
            return False
        op = cond["op"]
        val = cond["value"]
        return (
            (op == "<=" and metric_val <= val) or
            (op == "<"  and metric_val <  val) or
            (op == ">=" and metric_val >= val) or
            (op == ">"  and metric_val >  val) or
            (op == "==" and metric_val == val) or
            (op == "!=" and metric_val != val)
        )

    while current and visited < 200:
        visited += 1
        tool = TOOLS.get(graph["nodes"].get(current))
        log.append(f"Running node: {current}")

        if not tool:
            log.append(f"Tool not found: {graph['nodes'].get(current)}")
            RUNS[run_id]["status"] = "failed"
            _persist_run(run_id)
            return

        try:
            tool(state)
        except Exception as e:
            log.append(f"Exception: {e}")
            RUNS[run_id]["status"] = "failed"
            _persist_run(run_id)
            return

        RUNS[run_id]["state"] = state
        RUNS[run_id]["log"] = log.copy()
        _persist_run(run_id)

        if condition_satisfied():
            log.append("Loop stop condition satisfied.")
            RUNS[run_id]["status"] = "finished"
            _persist_run(run_id)
            return

        if state == prev_state:
            log.append("State unchanged — stopping.")
            RUNS[run_id]["status"] = "finished"
            _persist_run(run_id)
            return

        prev_state = copy.deepcopy(state)
        current = graph["edges"].get(current)

    RUNS[run_id]["status"] = "finished"
    log.append("Execution finished.")
    _persist_run(run_id)

# -------------------------
# API Endpoints
# -------------------------
@app.post("/graph/create")
async def create_graph(g: GraphDef):
    graph_id = str(uuid.uuid4())
    GRAPHS[graph_id] = g.dict()
    persistence.save_graph(graph_id, GRAPHS[graph_id])
    return {"graph_id": graph_id}

@app.post("/graph/run")
async def run_graph(payload: Dict[str, Any]):
    graph_id = payload.get("graph_id")
    if not graph_id:
        return {"error": "missing graph_id"}, 400

    graph = GRAPHS.get(graph_id) or persistence.load_graph(graph_id)
    if graph is None:
        return {"error": "graph_id not found"}, 400

    GRAPHS[graph_id] = graph

    init_state = payload.get("initial_state", {})
    run_id = str(uuid.uuid4())
    RUNS[run_id] = {"state": init_state, "log": [], "status": "running"}
    persistence.save_run(run_id, RUNS[run_id])

    asyncio.create_task(execute_graph(graph_id, run_id))
    return {"run_id": run_id}

@app.post("/graph/run_sync")
async def run_graph_sync(payload: Dict[str, Any]):
    graph_id = payload.get("graph_id")
    graph = GRAPHS.get(graph_id) or persistence.load_graph(graph_id)
    if graph is None:
        return {"error": "graph_id not found"}, 400

    GRAPHS[graph_id] = graph

    init_state = payload.get("initial_state", {})
    run_id = str(uuid.uuid4())
    RUNS[run_id] = {"state": init_state, "log": [], "status": "running"}
    persistence.save_run(run_id, RUNS[run_id])

    await execute_graph(graph_id, run_id)
    return RUNS[run_id]

@app.get("/graph/state/{run_id}")
async def get_run_state(run_id: str):
    run = RUNS.get(run_id) or persistence.load_run(run_id)
    if not run:
        return {"error": "run_id not found"}, 404

    RUNS[run_id] = run
    return run
