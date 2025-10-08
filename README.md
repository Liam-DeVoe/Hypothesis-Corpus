# Property-Based Testing Corpus Analysis Prototype

A prototype experimental setup for analyzing property-based testing patterns across a corpus of ~30k tests.

## Overview

This system analyzes property-based tests from GitHub repositories to understand:
- Types of properties (mathematical, model-based, round-trip, etc.)
- Generator usage patterns and composition
- Use of PBT features (assume, note, event, target)
- Testing methodologies and runners

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Worker 1  │     │   Worker 2  │     │   Worker N  │
│  (Docker)   │     │  (Docker)   │ ... │  (Docker)   │
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │                   │                   │
       └───────────────────┴───────────────────┘
                           │
                    ┌──────▼──────┐
                    │   SQLite    │
                    │   Database  │
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │  Streamlit  │
                    │  Dashboard  │
                    └─────────────┘
```

## Components

- **Worker System**: Parallel processing of repositories using multiprocessing
- **Docker Containers**: Isolated environments for running tests
- **SQLite Database**: Storage for analysis results
- **Streamlit Dashboard**: Real-time visualization of results

## Setup

Run the analysis:

```bash
docker build -f analyzer/Dockerfile -t pbt-analyzer --no-cache . && python run_analysis.py --dataset data/dataset.json --workers 6
```

View dashboard:
```bash
streamlit run dashboard/Overview.py
```

## Dataset Format

```json
{
  "owner/repo": {
    "node_ids": ["tests/file.py::TestClass::test_method"],
    "requirements.txt": "package==version\n..."
  }
}
```
