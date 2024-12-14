
# HRH Project - Scripts Directory

This directory contains essential scripts for the HRH project, focusing on data processing, analysis, and specialty data scraping.

## Contents

1. **Data Processing Utilities (`process.py`)**:
   - Import data from various formats (.csv, .xls, .xlsx).
   - Clean datasets and generate data dictionaries for better data understanding.
   - Detect and handle outliers using statistical methods:
     - Percentile thresholds.
     - Interquartile Range (IQR).
     - Moving window-based analysis.
     - Regression-based and leave-one-out (LOO) analysis.
   - Map unique values to user-defined categories and fill missing data based on references.
   - Validate and transform data types based on a predefined dictionary.

2. **Specialty Data Scraper (`scrp_spc.py`)**:
   - Scrape specialty data from the GIS Health Map website for multiple hospitals.
   - Process and clean scraped data, saving it in structured CSV format.
   - Automatically generate or update a `readme.txt` file documenting the scraping process.

## Usage

### **Data Processing Utilities (`process.py`)**
1. **Full Cleaning Pipeline**:
   - Use the `clean()` function to process a dataset:
     ```python
     from process import DataCleaning

     cleaned_df, raw_df, dict_path = DataCleaning.clean(
         file="example_dataset",
         data_dir="./data",
         output_dir="./output"
     )
     ```
2. **Custom Operations**:
   - Detect and handle outliers using specific methods:
     ```python
     from process import DataCleaning

     updated_df, has_outliers = DataCleaning.outlier_percentile(
         df=data, parameter='column_name', percentile=95, direction='more than', impute=None, print_outliers=True
     )
     ```
   - Fill missing data using:
     ```python
     from process import fill_missing

     updated_df = fill_missing(data, parameter="target_column", reference=["ref_column_1", "ref_column_2"])
     ```

3. **Generate Mappings**:
   - Create or update mappings for unique column values:
     ```python
     from process import mapping

     updated_df = mapping(data, parameter="category_column")
     ```

### **Specialty Data Scraper (`scrp_spc.py`)**
1. **Scrape Specialty Data**:
   - Use the `scrap()` function to scrape data:
     ```python
     from scrp_spc import scrap
     import pandas as pd

     hosp_id_example = pd.DataFrame({'hosp_id': ['001', '002']})
     spc_link_example = pd.DataFrame({'link': ['infopersonal', 'infohospital']})
     output_dir = "./output"

     output_file = scrap(hosp_id_example, spc_link_example, output_dir)
     print(f"Scraped data saved at: {output_file}")
     ```

2. **Output**:
   - The scraped data is saved in `output/spc.csv`.
   - A `readme.txt` file is created in the output directory with details of the scraping process.

## Dependencies

### For `process.py`:
- pandas
- numpy
- tqdm
- openpyxl
- sklearn
- statsmodels
- tabulate

### For `scrp_spc.py`:
- pandas
- requests
- beautifulsoup4
- tqdm

Ensure all dependencies are installed before executing the scripts:
```bash
pip install pandas numpy tqdm openpyxl sklearn statsmodels tabulate requests beautifulsoup4
```

## License

This project is licensed under the MIT License. See the [LICENSE](../LICENSE) file for details.

## Authors

- P. Sitthirat et al.

For detailed information on each function and class, refer to the docstrings within the scripts.
