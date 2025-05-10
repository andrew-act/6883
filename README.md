# Uniswap Data Pipeline

This project retrieves recent swap, mint, burn, and daily statistics data from the Uniswap v2 subgraph using GraphQL, processes the raw data, performs intelligent normalization, and outputs clean CSV files for analysis or machine learning tasks.

## Features

- 🔗 Fetches data from Uniswap v2 via The Graph API
- 🧹 Cleans and fills missing data (e.g., timestamps)
- 📊 Automatically detects and normalizes numerical fields using:
  - Log normalization
  - Min-Max scaling
  - Z-score standardization
- 📁 Outputs cleaned datasets to CSV files
- 🛠 Modular code structure for easy extension

## Requirements

- Python 3.7+
- Dependencies:
  ```bash
  pip install pandas numpy openpyxl requests
