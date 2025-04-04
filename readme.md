# ETL Pipeline

## Overview
This project is an **ETL (Extract, Transform, Load) Pipeline** built with Python and Pandas. The pipeline extracts data from CSV, JSON, or Excel files, transforms it by cleaning and standardizing columns, and then loads it into a specified destination file format.

## Features
- **Extract:** Reads data from CSV, JSON, or Excel files.
- **Transform:**
  - Drops missing values
  - Converts column names to lowercase
  - Adds an ETL timestamp
  - Assigns unique IDs if not present
- **Load:** Saves the transformed data into CSV, JSON, or Excel format.
- **Logging:** Logs the entire ETL process with error handling.

## Prerequisites
Ensure you have Python installed along with the following dependencies:

```bash
pip install pandas openpyxl
```

## File Structure
```
project/
│── data/
│   ├── source/       # Source data files
│   ├── destination/  # Processed output files
│── etl_pipeline.py   # Main ETL script
│── etl_pipeline.log  # Log file
```

## Usage
1. Modify `sample_source` and `sample_destination` paths in `etl_pipeline.py` to point to your actual data files.
2. Run the script:

```bash
python etl_pipeline.py
```

## Example
### Input (`data/source/my_data.csv`)
| Name   | Age | City    |
|--------|----|---------|
| Alice  | 25 | Mumbai  |
| Bob    |    | Delhi   |
| Charlie| 30 | Kolkata |

### Transformed Output (`data/destination/processed_data.csv`)
| id | name  | age | city    | etl_timestamp        |
|----|------|-----|---------|----------------------|
| 1  | alice| 25  | Mumbai  | 2025-04-04 12:00:00  |
| 2  | charlie| 30 | Kolkata | 2025-04-04 12:00:00  |

## Logging
The script generates a log file `etl_pipeline.log` to track the process and errors.

## License
This project is open-source and free to use.

## Contact
For issues or improvements, feel free to contribute!

