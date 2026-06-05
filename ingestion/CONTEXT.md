# General context
Here youll create all the files to develop a proper data extraction process, pulling from a public API and creating an initial parquet file. 
- Always use logging package instead of print to give logs.
- Youll work with parquet files.
- The raw data will be obtain from RAWG public API.
- set a main() as usual in all files.
- Work with pyspark when needed (data manipulation).

## Extraction 
Create a data_ingestion.py file to contain next functions:
- Raw data pull from RAWG public API.
- Parquet serialization (nested files to JSON strings).
- Extract 700 pages of 40 lines each from the API.
- Include a retry/backoff logic to deal with the 20req/s.
The final file .parquet call it bronze_data.

## Data push
Create a data_to_db.py file that will have required functions to:
- Read the data fo the bronze_data.parquet file.
- Connect to Databricks Community edition.
- Create a Bronze Delta table.
- Push the data of the parquet file into the Bronze Delta Table.
- Use .env credentials when needed.