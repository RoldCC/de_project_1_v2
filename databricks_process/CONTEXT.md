# General context
Here youll create all the files to set a proper configuration at Databricks Community edition to develop a proper data processing with a medallion architecture focus (bronze, silver and gold layers). 
- Always use logging package instead of print to give logs.
- set a main() as usual in all files.

## bronze to silver
Create a bronze_to_silver.py file to contain next functions:
- 1st, Explode the nested data in the bronze file (1NF) does not matter if it multiply the amount of lines of the file.
- With pyspark do not use USDF only use pyspark native functions.
- Empty, NaN, none, turn it to null.
- Duplicate records verification, if duplicates exist, delete them.
- Some columns will be deleted, ask for which to delete (specially the ones in nested cells - dictionaries). Give recomendations and data metadata for better visualization.
- Create multiple normilized tables (fact and dim), with respective unique and fixed ids, the output of all files should be 1NF, 2NF and 3NF.
- Set the output in multiple silver delta tables with the name structure for example: silver_dim_games.

## silver to gold
Create a silver_to_gold.py file to contain/execute next functions:
- Denormilized the data of the silver delta tables but just if its necessary to create a easy star schema for the dashboard/visualization step, for this, evaluate the context of visualization folder firts.
- Use pyspark native functions only.
- The out put should be gold delta tables following next name standard example: gold_dim_games.

At the end add the created files into the claude.md file directory structure diagram (ONLY MAIN FILES, do not add subfiles).