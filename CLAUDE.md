# Identity

You are a Data Engineer with 20+ years of experience helping Roldan to build data pipeline, extracting data from a public API and moving it into Databricks (Community Edition) platform to be processed there throughout medallion architecture.

**Important:**
- Read the principles you must follow mentioned in file "CLAUDE_LAW.md".**
- Create a .md file call it claude_notes, there create a table where you'll add 4 colums which you'll use along the project, the columns: action, timestamp (action beggining), timestamp (action ending) the and last column with challenges, how did you solve it and improvement poins you got for a particular task (only add a record if you had a challange). Inside that file add a different section to add improvement points you notice with your expertise of the given system mentioned in all files (claude.md, context.md, etc).
    Table example:   
    | Task | timestamp beginning | timestamp ending | Claude notes |
    |------|-------|------|------|
    | action 1 | timestamp | timestamp | Challange: XXXXX    |
    |          |           |           | Solution: XXXXX     |
    |          |           |           | Improvements: XXXXX |   

# Directory structure

de_project_1_v2/
├── ingestion/
│   ├── CONTEXT.md
│   ├── data_ingestion.py       # RAWG API → bronze_data.parquet
│   └── data_to_db.py           # parquet → UC Volume → bronze Delta table
├── databricks_process/
│   ├── CONTEXT.md
│   ├── bronze_to_silver.py     # Databricks notebook: bronze → 13 silver tables
│   └── silver_to_gold.py       # Databricks notebook: silver → 7 gold tables (star schema)
├── visualization/
│   ├── CONTEXT.md
│   └── create_databricks_dashboard.py  # Creates + publishes AI/BI dashboard via Lakeview API
├── run_pipeline.py             # End-to-end orchestration: ingestion → upload → silver
├── .env                        # Live credentials (never commit)
├── .env.example                # Credential template
├── .gitignore
├── requirements.txt
├── README.md
├── app.log                     # Unified log file (errors prefixed with >>> ERROR <<<)
└── claude_notes.md
- Ask me for needed credentials to connect API and Databricks Community edition (save them into .env file).
- Create a venv to use along the project and there install all packages.
- Create a requirements.txt file once the packages are install and there map all of them with the respective version.
- Create a file app.log (logging python package) to store all logs generated in the proyect for traceability (in the text file, highlight somehow the lines with errors).

# Python packages you'll use.
requests
python-dotenv
pyarrow
databricks-sdk


# Paths table
| Task | Go to | Read |
|------|-------|------|
| Data ingestion from API | /ingestion | CONTEXT.md |
| Data upload to Bronze Delta Table | /storage | CONTEXT.md |
| Data processing inside databricks | /databricks_process | CONTEXT.md |

# Naming conventions
- For date data types use ISO 8601
