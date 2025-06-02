# apple product analysis

This project analyzes customer purchasing behavior to determine how many people bought AirPods after purchasing a Mac or iPhone. It utilizes data extraction, transformation, and loading (ETL) processes to prepare the data for analysis.

## Project Structure

- `apple-analysis.py`: Main script to execute the analysis.
- `extractor.py`: Handles data extraction from source files.
- `transformer.py`: Performs data transformation and cleaning.
- `loader.py`: Loads the transformed data into the desired format or database.
- `reader_factory.py`: Factory pattern implementation for reading different data sources.
- `loader_factory.py`: Factory pattern implementation for loading data into various destinations.

## Technologies Used

- Python
- Pandas
- Databricks
- Pyspark
