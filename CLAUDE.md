# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Property-Based Testing (PBT) Corpus Analysis system that analyzes Hypothesis test patterns across GitHub repositories. It uses Docker containers for isolated test execution and provides real-time visualization of analysis results.

## Commands

### Running Analysis
```bash
# Build Docker image and run analysis
docker build -t pbt-analyzer . && python run_analysis.py --dataset data/dataset.json --workers 4

# Run sample test with MarkCBell/bigger repository
python run_analysis.py --sample

# Run with limited repositories
python run_analysis.py --dataset data/dataset.json --limit 10 --workers 2

# Start visualization dashboard
streamlit run dashboard.py
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
- Virtual environment path must match Python version: `/tmp/venv/lib/python3.13/site-packages`
- Network access is required for pip installation of repository dependencies
- Container runs as non-root user `testrunner` for security

#### Pattern Detection System
The analyzer detects:
- **60+ Hypothesis strategies** through regex patterns
- **Property types**: mathematical, round-trip, model-based, oracle, metamorphic
- **Feature usage**: assume, note, event, target, settings, max_examples
- **Custom strategies** through AST analysis

#### Database Schema
8 interconnected tables track:
- Repository metadata and processing status
- Individual test information
- Generator usage with composition patterns
- Property type classifications
- Feature adoption metrics
- Full source code storage
- Test runner information
- Analysis run metadata

### Configuration

The system uses `config.yaml` for:
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