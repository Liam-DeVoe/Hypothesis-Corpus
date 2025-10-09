# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Property-Based Testing (PBT) Corpus Analysis system that analyzes Hypothesis test patterns across GitHub repositories. It uses Docker containers for isolated test execution and provides real-time visualization of analysis results.

## Commands

### Collecting Repositories
```bash
# Step 1: Collect repositories from GitHub
python run.py collect

# Step 2: Install repositories and collect test node IDs from a limit number of repos
python run.py install --limit 10
```

### Running Analysis
```bash
# Run analysis (pulls from database)
python run.py analysis --workers 4

# Run sample test with MarkCBell/bigger repository
python run.py analysis --sample

# Run with limited repositories
python run.py analysis --limit 10 --workers 2

# Start visualization dashboard
streamlit run dashboard/Overview.py
```

### Running Tasks
```bash
# Run clustering task (Clio-style analysis of patterns/domains)
python run.py task run clustering

# Clear task data
python run.py task clear --task-name clustering
```

### Rebuilding Docker
```bash
docker build -f analysis/Dockerfile -t pbt-analysis .
```

### Development & Debugging
```bash
# Check database contents
sqlite3 analysis/data.db ".tables"
sqlite3 analysis/data.db "SELECT * FROM core_repository LIMIT 5;"
```

## Architecture

### Core Flow
1. **python run.py collect** collects repositories from GitHub and stores in `core_repository` table
2. **python run.py install** clones repos, installs dependencies, collects test nodes, updates database
3. **python run.py analysis** reads from database and orchestrates the analysis pipeline
4. **WorkerPool** (analysis/worker.py) distributes repositories across multiple processes
5. Each worker uses **TestRunner** (analysis/test_runner.py) to:
   - Clone repository into temporary directory
   - Copy experiment modules into repo directory
   - Create config.json with node_ids and experiment configuration
   - Execute runner.py in Docker container with network access
   - Parse results.json output
6. Results stored in SQLite database (analysis/database.py)
7. **dashboard/Overview.py** provides real-time Streamlit visualization

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
The unified database (`analysis/data.db`) contains collection and analysis tables:

**Collection tables** (for GitHub repository discovery):
- `core_repository`: Collected repositories from GitHub search (includes `full_name`, `requirements`, metadata)
- `core_minhashes`: MinHash data for deduplication

**Analysis tables** (for test analysis):
- `core_node`: Individual test information (references core_repository)

Experiment-specific tables are defined by each experiment's `get_schema_sql()`:
- `runtime` experiment: `runtime_summary` (execution metadata + coverage JSON), `runtime_testcase` (per-testcase coverage with cumulative lines)
- `facets` experiment: `facets` table with summaries, property patterns, and technical domains

Task-specific tables are defined by each task's `get_schema_sql()`:
- `clustering` task: `facet_clusters`, `facet_cluster_assignment`

### Configuration

The system uses **CLI arguments** for configuration (no config files):
- `--db-path` (default: `analysis/data.db`): Database path
- `--workers` (default: `4`): Number of worker processes
- `--docker-image` (default: `pbt-analysis:latest`): Docker image for analysis

The system uses `analysis/secrets.json` for API tokens:
```json
{
  "claude_code_oauth_token": "your-token-here",
  "github_token": "your-token-here"
}
```

See `analysis/secrets.json.example` for a template.

### Database-Driven Workflow

The analysis system pulls repositories directly from the `core_repository` table in `analysis/data.db`:
- **Step 1**: Repositories are collected via `python run.py collect` and stored in `core_repository`
- **Step 2**: `python run.py install` processes each repo:
  - Clones the repository
  - Runs installation in Docker container (isolated, reproducible)
  - Installs dependencies (guessing at common extras and requirements files)
  - Collects pytest node IDs via `pytest --collect-only`
  - Updates `requirements` and `node_ids` columns in database
- **Step 3**: `python run.py analysis` reads from the database and runs experiments
- Test discovery happens automatically if `node_ids` are not pre-specified

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

- **run.py**: Unified CLI interface with collect/analysis/task subcommands
- **analysis/test_runner.py**: Orchestrates cloning, environment setup, and Docker execution
- **analysis/worker.py**: Multiprocessing orchestration and error handling
- **analysis/experiments/runner.py**: Entry point that runs inside Docker containers
- **analysis/database.py**: Schema definition and data persistence

### Experiments & Tasks
- **analysis/experiments/**: Experiment implementations (runtime, facets)
- **analysis/tasks/**: Task implementations (clustering) and runner logic
- **analysis/collect/**: GitHub repository collection scripts
