# Nisaba

Nisaba is an agentic economic analysis harness for the ABS.

The point of this repo is not just to expose the ABS through MCP. The point is to wrap that underlying server with a harness that behaves more like a real economic analyst:

- a curated ABS catalog and structure layer so the agent starts from known working dataset patterns instead of inventing brittle API calls
- a planning loop that reasons about what data is needed before retrieving it
- a Python sandbox for filtering, reshaping, joining, calculating, checking, and preparing chart-ready data
- a frontend that renders conversational answers, tables, and charts
- an economic-analysis prompt layer that knows the difference between time series, panel data, and matrix-style tables such as supply-use tables

This makes it much more than a raw model sitting on top of the ABS API. Nisaba is designed to think about the structure of economic data, work out what needs to be retrieved, and then do the analysis properly.

In practice, Nisaba can:

- retrieve curated ABS data directly
- inspect and compare multiple datasets
- calculate derived metrics like ratios, rankings, and per-worker measures
- handle matrix tables with explicit row/column logic
- generate charts and tables automatically when they help explain the answer

Underneath that, the repo still includes the raw MCP-style ABS server layer. But the harness is the main product, and the MCP server is the substrate it sits on.

The stack includes:

- GPT-5.4-driven orchestration
- curated ABS retrieval logic
- web-search support for broader context when needed
- one Python sandbox tool
- React frontend
- FastAPI backend

Produced by Dottie AI Studio · Powered by mcp-server-abs.

## Requirements

- Python
- Node.js + npm
- `OPENAI_API_KEY` in `.env`

Example `.env`:

```env
OPENAI_API_KEY=your_key_here
OPENAI_MODEL=gpt-5.4
OPENAI_REASONING_EFFORT=low
MAX_LOOPS=15
```

## Local Dev

Frontend hot reload on `http://localhost:3000`  
Backend auto-reload on `http://127.0.0.1:8000`

First run:

```powershell
cd "C:\Users\jorda\OneDrive\Documents\Dottie\abs-mcp"; .\start-dev.ps1
```

Later runs:

```powershell
cd "C:\Users\jorda\OneDrive\Documents\Dottie\abs-mcp"; .\start-dev.ps1 -SkipInstall
```

## Local Demo

Single built app served from one backend on `http://localhost:3000`

First run:

```powershell
cd "C:\Users\jorda\OneDrive\Documents\Dottie\abs-mcp"; .\start-demo.ps1
```

Later runs without reinstall/rebuild:

```powershell
cd "C:\Users\jorda\OneDrive\Documents\Dottie\abs-mcp"; .\start-demo.ps1 -SkipInstall -SkipBuild
```

## Files

- [start-dev.ps1](/mnt/c/Users/jorda/OneDrive/Documents/Dottie/abs-mcp/start-dev.ps1): local dev with hot reload
- [start-demo.ps1](/mnt/c/Users/jorda/OneDrive/Documents/Dottie/abs-mcp/start-demo.ps1): built local demo
- [run.py](/mnt/c/Users/jorda/OneDrive/Documents/Dottie/abs-mcp/run.py): Python entrypoint used by the demo flow

## Core idea

The harness prefers the curated layer first:

- [CURATED_ABS_CATALOG.txt](/mnt/c/Users/jorda/OneDrive/Documents/Dottie/abs-mcp/CURATED_ABS_CATALOG.txt)
- [CURATED_ABS_STRUCTURES.txt](/mnt/c/Users/jorda/OneDrive/Documents/Dottie/abs-mcp/CURATED_ABS_STRUCTURES.txt)

These files encode tested dataset descriptions, query templates, and guidance about what is literally available in the returned ABS data.

That curated layer is what makes the system dependable. Instead of asking the model to infer arbitrary ABS dimensions and codes, it starts from validated patterns and then uses sandbox analysis to narrow, combine, and interpret the results.

## Notes

- Dev mode uses Vite on port `3000` and proxies API calls to backend port `8000`.
- Demo mode builds the frontend and serves it directly from FastAPI on port `3000`.
- If PowerShell blocks scripts, run:

```powershell
Set-ExecutionPolicy -Scope Process Bypass
```
