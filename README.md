# Persistent Workflow Execution Engine (Task 2)

This submission implements a workflow engine with durable state.

## Features
- Graph create/run/run_sync endpoints
- Tools: profile, detect_anomalies, generate_rules, apply_rules
- Looping and safe stop conditions
- FULL persistence using SQLite:
  - Graphs stored in DB
  - Runs stored + restored after server restart
- Clean, readable code without notebook clutter

## Files
- app/app.py — main FastAPI engine
- app/persistence.py — SQLite storage layer
- requirements.txt — library list# Tredence
