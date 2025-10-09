# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Property-Based Testing (PBT) Corpus Analysis system that analyzes Hypothesis test patterns across GitHub repositories. It uses Docker containers for isolated test execution and provides real-time visualization of analysis results.

## Commands

### Running Analysis
```bash
# run analysis
python run_analysis.py --dataset data/dataset.json --workers 4

# Run sample test with MarkCBell/bigger repository
python run_analysis.py --sample

# Run with limited repositories
python run_analysis.py --dataset data/dataset.json --limit 10 --workers 2

# Start visualization dashboard
streamlit run dashboard/Overview.py
```

### Rebuilding docker

```
docker build -f analysis/Dockerfile -t pbt-analysis .
```

### Running Tasks
```bash
# Run clustering task (Clio-style analysis of patterns/domains)
python run_tasks.py run clustering

# Clear task data
python run_tasks.py clear --task clustering
```

### Development & Debugging
```bash
# Check database contents
sqlite3 data/analysis.db ".tables"
sqlite3 data/analysis.db "SELECT * FROM repositories LIMIT 5;"
```

## Architecture

### Core Flow
1. **run_analysis.py** orchestrates the entire analysis pipeline
2. **WorkerPool** (analysis/worker.py) distributes repositories across multiple processes
3. Each worker uses **TestRunner** (analysis/test_runner.py) to:
   - Clone repository into temporary directory
   - Copy experiment modules into repo directory
   - Create config.json with node_ids and experiment configuration
   - Execute runner.py in Docker container with network access
   - Parse results.json output
4. Results stored in SQLite database (analysis/database.py)
5. **dashboard/Overview.py** provides real-time Streamlit visualization

### Critical Implementation Details

#### Docker Container Execution
- The TestRunner copies experiment modules (runner.py, experiment.py, utils.py, {experiment_name}.py) into the repo directory before containerization
- Files are packaged into a tar archive and sent to the container via docker API (avoiding Mac mount penalties)
- Each Docker container provides isolation - dependencies installed directly into container Python environment
- Network access is required for pip installation of repository dependencies
- Container runs runner.py which reads config.json, installs dependencies, and runs the specified experiment

#### Experiment System
**Experiments** run in Docker containers to analyze tests. Each experiment:
- Inherits from `Experiment` base class (analysis/experiments/experiment.py)
- Auto-registers via `__init_subclass__` using the `name` class attribute
- Implements `get_schema_sql()`, `run()`, `delete_data()`, `store_to_database()`
- Runs inside container via runner.py (analysis/experiments/runner.py)
- Returns results as dict which are stored in results.json

Built-in experiments:
- `runtime`: Detects Hypothesis strategies, features, and execution data
- `facets`: Uses Claude to generate summaries, property patterns, and technical domains

#### Tasks System
**Tasks** run after experiments to analyze their results:
- Inherit from `Task` base class (analysis/tasks/task.py)
- Auto-register via `__init_subclass__` using the `name` class attribute
- Declare `follows = ["experiment_name"]` to automatically run after experiments
- Implement `get_schema_sql()`, `run()`, `store_to_database()`, `delete_data()`
- Store results in separate tables for dashboard visualization

Built-in tasks:
- `clustering`: Uses all-mpnet-base-v2 embeddings and k-means to cluster patterns/domains (Clio-style)

Clustering implementation:
- Embeds facets using sentence-transformers (all-mpnet-base-v2, 768-dimensional)
- Determines optimal k based on dataset size: `k = sqrt(n) * factor`
- Uses Claude CLI to generate human-readable cluster names and descriptions
- Model is cached at class level to avoid reloading

#### Database Schema
Core tables track repository analysis:
- `repositories`: Repository metadata and processing status
- `nodes`: Individual test information

Experiment-specific tables are defined by each experiment's `get_schema_sql()`:
- `runtime` experiment: `runtime_summary` (execution metadata + coverage JSON), `runtime_testcase` (per-testcase coverage with cumulative lines)
- `facets` experiment: `facets` table with summaries, property patterns, and technical domains

Task-specific tables are defined by each task's `get_schema_sql()`:
- `clustering` task: `facet_clusters`, `facet_cluster_assignments`

### Configuration

The system uses a top-level `secrets.json` file for API tokens:
```json
{
  "claude_code_oauth_token": "your-token-here",
  "github_token": "your-token-here"
}
```

### Dataset Format

Input datasets must follow this JSON structure:
```json
{
  "owner/repo": {
    "node_ids": ["tests/file.py::TestClass::test_method"],
    "requirements.txt": "package==version\n..."
  }
}
```

## Common Issues & Solutions

### Analysis Failing
1. Check Docker container logs are being captured properly
2. Ensure Python version in runner.py matches Docker image (currently 3.13)
3. Verify network access is enabled for pip installations
4. Check that experiment modules are being copied correctly

### Adding New Experiments
1. Create class inheriting from `Experiment` in analysis/experiments/
2. Set `name` class attribute for auto-registration
3. Implement `get_schema_sql()`, `run()`, `store_to_database()`, `delete_data()`
4. Ensure imports work both as package and standalone (use try/except pattern)
5. Experiment runs inside container, so all dependencies must be installable via pip
6. Rebuild Docker image if adding system dependencies

### Creating New Tasks
1. Create class inheriting from `Task` in analysis/tasks/
2. Set `name` class attribute for auto-registration
3. Implement `get_schema_sql()`, `run()`, `store_to_database()`, `delete_data()`
4. Set `follows = ["experiment_name"]` to declare dependencies
5. Export from analysis/tasks/__init__.py

### Database Operations
The database uses context managers for all operations:
```python
with db.connection() as conn:
    cursor = conn.execute(query, params)
    conn.commit()
```

## Key Files to Understand

- **analysis/test_runner.py**: Orchestrates cloning, environment setup, and Docker execution
- **analysis/worker.py**: Multiprocessing orchestration and error handling
- **analysis/experiments/runner.py**: Entry point that runs inside Docker containers
- **analysis/database.py**: Schema definition and data persistence
- **run_analysis.py**: CLI interface and main orchestration

### Experiments & Tasks
- **analysis/experiments/**: Experiment implementations (runtime, facets)
- **analysis/tasks/**: Task implementations (clustering) and runner logic
- **run_tasks.py**: CLI for manually running tasks
