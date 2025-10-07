# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Property-Based Testing (PBT) Corpus Analysis system that analyzes Hypothesis test patterns across GitHub repositories. It uses Docker containers for isolated test execution and provides real-time visualization of analysis results.

## Commands

### Running Analysis
```bash
# Build Docker image and run analysis
docker build -f analyzer/Dockerfile -t pbt-analyzer . && python run_analysis.py --dataset data/dataset.json --workers 4

# Run sample test with MarkCBell/bigger repository
python run_analysis.py --sample

# Run with limited repositories
python run_analysis.py --dataset data/dataset.json --limit 10 --workers 2

# Start visualization dashboard
streamlit run dashboard/Overview.py
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
# Run debug scripts for troubleshooting Docker execution
python debug_docker.py

# Check database contents
sqlite3 data/analysis.db ".tables"
sqlite3 data/analysis.db "SELECT * FROM repositories LIMIT 5;"
```

## Architecture

### Core Flow
1. **run_analysis.py** orchestrates the entire analysis pipeline
2. **WorkerPool** (analyzer/worker.py) distributes repositories across multiple processes
3. Each worker uses **TestRunner** (analyzer/test_runner.py) to:
   - Clone repository into temporary directory
   - Create setup.sh to install dependencies in Docker container
   - Generate analyze.py script with embedded analysis logic
   - Execute in Docker container with network access
   - Parse results.json output
4. Results stored in SQLite database (analyzer/database.py)
5. **dashboard.py** provides real-time Streamlit visualization

### Critical Implementation Details

#### Docker Container Execution
- The TestRunner dynamically generates an analyze.py script that is injected into each container
- The analysis script is a self-contained Python program with all pattern detection logic embedded
- Each Docker container provides isolation - no virtual environments needed
- Network access is required for pip installation of repository dependencies
- Dependencies are installed directly into the container's Python environment

#### Pattern Detection System
The analyzer detects:
- **60+ Hypothesis strategies** through regex patterns
- **Property types**: mathematical, round-trip, model-based, oracle, metamorphic
- **Feature usage**: assume, note, event, target, settings, max_examples
- **Custom strategies** through AST analysis

#### Experiments & Tasks System
**Experiments** run in Docker containers to analyze tests:
- `coverage`: Detects Hypothesis strategies and features
- `facets`: Uses Claude to generate summaries, property patterns, and technical domains

**Tasks** run after experiments to analyze their results:
- `clustering`: Uses all-mpnet-base-v2 embeddings and k-means to cluster patterns/domains (Clio-style)
- Tasks declare `follows = ["experiment_name"]` to automatically run after experiments
- Store results in separate tables for dashboard visualization

Clustering implementation:
- Embeds facets using sentence-transformers (all-mpnet-base-v2, 768-dimensional)
- Determines optimal k based on dataset size: `k = sqrt(n) * factor`
- Uses Claude to generate human-readable cluster names and descriptions
- Model is cached at class level to avoid reloading

#### Database Schema
Core tables track repository analysis:
- Repository metadata and processing status
- Individual test information
- Generator usage with composition patterns
- Property type classifications
- Feature adoption metrics
- Full source code storage

Additional tables for experiments and tasks:
- `facets`: Summaries, property patterns, and technical domains (by facets experiment)
- `facet_clusters` & `facet_cluster_assignments`: Cluster metadata and assignments (by clustering task)

### Configuration

The system uses `analyzer/config.yaml` for:
```yaml
database:
  path: "data/analysis.db"
docker:
  image: "pbt-analyzer:latest"
workers:
  max_workers: 4
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
2. Ensure Python version in analyze.py matches Docker image (currently 3.13)
3. Verify network access is enabled for pip installations
4. Check that setup.sh has correct venv activation

### Adding New Pattern Detection
1. Modify the `_create_analysis_script` method in analyzer/test_runner.py
2. Update corresponding database schema if tracking new metrics
3. Regenerate Docker image after changes

### Creating New Tasks
1. Create class inheriting from `Task` in analyzer/tasks/
2. Implement `get_schema_sql()`, `run()`, `store_to_database()`, `delete_data()`
3. Set `follows = ["experiment_name"]` to declare dependencies
4. Export from analyzer/tasks/__init__.py

### Database Operations
The database uses context managers for all operations:
```python
with db.connection() as conn:
    cursor = conn.execute(query, params)
    conn.commit()
```

## Key Files to Understand

- **analyzer/test_runner.py**: Contains the entire analysis logic embedded in `_create_analysis_script()` method
- **analyzer/worker.py**: Multiprocessing orchestration and error handling
- **analyzer/database.py**: Schema definition and data persistence
- **run_analysis.py**: CLI interface and main orchestration

### Experiments & Tasks
- **analyzer/experiments/**: Experiment implementations (coverage, facets)
- **analyzer/tasks/**: Task implementations (clustering) and runner logic
- **run_tasks.py**: CLI for manually running tasks
