# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Property-Based Testing (PBT) Corpus Analysis system that analyzes Hypothesis test patterns across GitHub repositories. It uses Docker containers for isolated test execution and provides real-time visualization of analysis results.

## Commands

### Collecting Repositories
```bash
# Step 1: Collect repositories from GitHub and store in database
python run.py collect

# Step 2: Install repositories and collect test node IDs
python run.py install --limit 10

# Install with debug output (shows container logs)
python run.py install --limit 10 --debug
```

### Running Experiments
```bash
# Run experiments (reads from database, requires 'valid' status repos)
python run.py experiment --workers 4

# Run specific experiment(s)
python run.py experiment -e runtime -e facets

# Run with limited repositories
python run.py experiment --limit 10 --workers 2
```

### Running Tasks
```bash
# Run clustering task (Clio-style analysis of patterns/domains)
python run.py task run clustering

# Clear task data
python run.py task clear --task-name clustering
```

### Dashboard
```bash
# Start visualization dashboard (default port: 8501)
python run.py dashboard

# Use different database or port
python run.py dashboard --db-path test.db --port 8502

# Direct streamlit command (alternative)
streamlit run dashboard/Overview.py -- --db-path analysis/data.db
```

### Docker & Development
```bash
# Rebuild Docker image
./build.sh image

# Check database contents
sqlite3 analysis/data.db ".tables"
sqlite3 analysis/data.db "SELECT * FROM core_repository LIMIT 5;"

# Add commit_hash column to existing database (migration)
sqlite3 analysis/data.db "ALTER TABLE core_repository ADD COLUMN commit_hash TEXT"
```

## Architecture

### Core Flow
1. **python run.py collect** collects repositories from GitHub and stores in `core_repository` table
2. **python run.py install** processes repositories:
   - Clones repository to temporary directory
   - Copies `_install.py` script into repository
   - Creates tar archive and sends to Docker container
   - Container installs dependencies and runs pytest plugin to collect node IDs
   - Captures git commit hash for reproducibility
   - Updates database with `requirements`, `node_ids`, `commit_hash`, and `status='valid'`
3. **python run.py experiment** reads valid repos from database and orchestrates analysis pipeline
4. **WorkerPool** (analysis/worker.py) distributes repositories across multiple processes
5. Each worker uses **TestRunner** (analysis/test_runner.py) to:
   - Clone repository into temporary directory
   - Copy experiment modules into repo directory
   - Create config.json with node_ids and experiment configuration
   - Execute runner.py in Docker container with network access
   - Parse results.json output
6. Results stored in SQLite database (analysis/database.py)
7. **dashboard/overview.py** provides real-time Streamlit visualization

### Critical Implementation Details

#### Docker Container Execution
- The TestRunner copies experiment modules (runner.py, experiment.py, utils.py, {experiment_name}.py) into the repo directory before containerization
- Files are packaged into a tar archive and sent to the container via docker API (avoiding Mac mount penalties)
- Each Docker container provides isolation - dependencies installed directly into container Python environment
- Network access is required for pip installation of repository dependencies
- Container runs runner.py which reads config.json, installs dependencies, and runs the specified experiment

#### Installation & Node Collection
- Uses custom pytest plugin approach (not subprocess text parsing) for collecting test node IDs
- Plugin hooks into `pytest_collection_finish` to get clean node IDs from `session.items`
- Git commit hash captured via `git rev-parse HEAD` after configuring safe.directory to avoid dubious ownership errors
- Docker container may have different user/ownership than host, requiring `git config --global --add safe.directory /app`

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
The unified database (`analysis/data.db`) contains collection and analysis tables:

**Collection tables** (for GitHub repository discovery):
- `core_repository`: Collected repositories from GitHub search (includes `full_name`, `requirements`, `node_ids`, `commit_hash`, `status`, metadata)
- `core_minhashes`: MinHash data for deduplication

**Analysis tables** (for test analysis):
- `core_node`: Individual test information (references core_repository)

Experiment-specific tables are defined by each experiment's `get_schema_sql()`:
- `runtime` experiment: `runtime_summary` (execution metadata + coverage JSON), `runtime_testcase` (per-testcase coverage with cumulative lines)
- `facets` experiment: `facets` table with summaries, property patterns, and technical domains

Task-specific tables are defined by each task's `get_schema_sql()`:
- `clustering` task: `facets_cluster`, `facets_cluster_assignment`

#### Database API
The Database class maintains a **single persistent connection** per db_path (singleton pattern):
- `db.execute(query, params)` - execute query, returns cursor
- `db.fetchone(query, params)` - execute and return single row
- `db.fetchall(query, params)` - execute and return all rows
- `db.commit()` - commit transaction
- `db._conn` - direct connection access (for pandas queries only)

For pandas SQL queries:
```python
df = pd.read_sql_query("SELECT ...", db._conn)
```

### Configuration

The system uses **CLI arguments** for configuration (no config files):
- `--db-path` (default: `analysis/data.db`): Database path
- `--workers` (default: `4`): Number of worker processes
- `--docker-image` (default: `pbt-analysis:latest`): Docker image for analysis

The system uses a top-level `secrets.json` file for API tokens:
```json
{
  "claude_code_oauth_token": "your-token-here",
  "github_token": "your-token-here"
}
```

### Database-Driven Workflow

The analysis system pulls repositories directly from the `core_repository` table in `analysis/data.db`:
- **Step 1**: Repositories are collected via `python run.py collect` and stored in `core_repository`
- **Step 2**: `python run.py install` processes repos with `status IS NULL`:
  - Clones the repository
  - Runs `_install.py` in Docker container (isolated, reproducible)
  - Installs dependencies (tries editable install, common extras, and requirements files)
  - Collects pytest node IDs using custom plugin (not text parsing)
  - Captures git commit hash
  - Updates `requirements`, `node_ids`, `commit_hash` columns, sets `status='valid'`
- **Step 3**: `python run.py experiment` selects repos `WHERE status = 'valid'` and runs experiments

## Common Issues & Solutions

### Analysis Failing
1. Check Docker container logs with `--debug` flag
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
The database uses a singleton pattern with persistent connections:
```python
db = Database(db_path="analysis/data.db")

# Execute queries
db.execute("INSERT INTO ...", (val1, val2))
db.commit()

# Fetch data
row = db.fetchone("SELECT * FROM ... WHERE id = ?", (id,))
rows = db.fetchall("SELECT * FROM ...")

# For pandas
import pandas as pd

df = pd.read_sql_query("SELECT ...", db._conn)
```

## Key Files to Understand

- **run.py**: Unified CLI interface with collect/install/experiment/task/dashboard commands
- **analysis/collect/install_repos.py**: Repository installation orchestration (cloning, Docker execution)
- **analysis/collect/_install.py**: Script that runs inside Docker to install deps and collect nodes
- **analysis/test_runner.py**: Orchestrates cloning, environment setup, and Docker execution for experiments
- **analysis/worker.py**: Multiprocessing orchestration and error handling
- **analysis/experiments/runner.py**: Entry point that runs inside Docker containers
- **analysis/database.py**: Schema definition and data persistence with singleton connection pattern

### Experiments & Tasks
- **analysis/experiments/**: Experiment implementations (runtime, facets)
- **analysis/tasks/**: Task implementations (clustering) and runner logic
- **analysis/collect/**: GitHub repository collection scripts

### Dashboard
- **dashboard/overview.py**: Main dashboard entry point (Streamlit app)
- **dashboard/pages/**: Individual dashboard pages (repositories, runtime, facets, clusters, summary)
- **dashboard/utils.py**: Shared utilities for dashboard pages
