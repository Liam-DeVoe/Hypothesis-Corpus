image:
    docker build -f analysis/Dockerfile -t pbt-analysis .

install *args:
    python3.13 run.py install {{args}}

experiment *args:
    python3.13 run.py experiment {{args}}

task *args:
    python3.13 run.py task {{args}}

collect:
    python3.13 run.py collect

dashboard *args:
    python3.13 run.py dashboard {{args}}

mark_invalid *args:
    python3.13 run.py mark_invalid {{args}}

sankey *args:
    python3.13 sankey/sankey.py {{args}}
