"""
Specialty Data Scraping Utility

This script contains functionality to scrape specialty-related data from the Thai Ministry of Public Health's health map website and save it as a structured CSV file. It is designed to automate the process of collecting and organizing data for analysis.

Key functionalities include:
- Scraping specialty data for multiple hospitals using their unique hospital IDs and specialty links.
- Extracting and processing table data from HTML pages.
- Detecting and handling missing or incomplete data.
- Saving results incrementally to avoid data loss during long scraping sessions.
- Generating a README file with details about the scraping process.

Dependencies:
- pandas
- requests
- BeautifulSoup4
- tqdm
- datetime
- time

Usage:
1. Prepare input data as pandas DataFrames: `hosp_id` for hospital IDs and `spc_link` for specialty links.
2. Specify the output folder to save scraped results and log files.
3. Call the `scrap()` function with the prepared inputs.

Example:
    from scrp_spc import scrap
    
    hosp_id_example = pd.DataFrame({'hosp_id': ['001', '002']})
    spc_link_example = pd.DataFrame({'link': ['infopersonal', 'infohospital']})
    output_dir = "./output"

    output_file = scrap(hosp_id_example, spc_link_example, output_dir)
    print(f"Scraped data is available in: {output_file}")

Authors: P. Sitthirat et al
Version: 1.0
License: MIT License
"""

# Standard library imports
import os

# Third-party library imports
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from datetime import datetime
import time

def scrap(hosp_id, spc_link, output_folder):
    """
    Scrape specialty data from a specified health map website and save it as a CSV file.

    Parameters:
    - hosp_id (DataFrame): A DataFrame containing hospital IDs.
    - spc_link (DataFrame): A DataFrame containing specialty links.
    - output_folder (str): The directory to save the output files.

    Returns:
    - str: The path to the saved CSV file.
    """
    
    # Create an empty DataFrame to store results
    spc_df = pd.DataFrame()
    N = 1

    # Ensure the output folder exists
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Initialize tqdm progress bar
    with tqdm(total=len(hosp_id), desc="Progress") as pbar:
        for index_hosp, row_hosp in hosp_id.iterrows():
            for index_link, row_link in spc_link.iterrows():
                
                # Extract the specialty data from URL
                hosp_url = f"http://gishealth.moph.go.th/healthmap/{row_link['link']}.php?maincode={row_hosp['hosp_id']}"

                # Determine table structure based on specialty type
                n_select, n_table = (3, 5) if row_link['link'] == 'infopersonal' else (2, 4)
                
                # Retry mechanism for GET requests
                success = False
                while not success:
                    try:
                        # GET request to the URL with retry mechanism
                        response_hosp = requests.get(hosp_url)
                        success = True
                    except requests.exceptions.RequestException as e:
                        print(f"Failed to fetch data for hosp_id {row_hosp['hosp_id']} with error: {e}")
                        print("Retrying...")
                        time.sleep(5)  # Wait before retrying
                    
                        
                # Parse the HTML content of the page
                soup_hosp = BeautifulSoup(response_hosp.text, 'html.parser')

                # Find all tables in the updated page
                tables = soup_hosp.find_all('table')

                # Check the availability of tables
                check_table = tables[n_select]
                select_element = check_table.find('select')
                options = select_element.find_all('option')
                check_data = [{'id': option.get('value'), 'check': option.text.strip()} for option in options]

                        
                # Check if "ไม่มีการบันทึกข้อมูล"
                if check_data[0]['check'] != "ไม่มีการบันทึกข้อมูล":
                            
                    for point in check_data:
                        
                        id = point['id']
                        time_input = point['check']
                        
                        point_url = f"{hosp_url}&id={id}"
                        
                        success = False
                        while not success:
                            try:
                                response_point = requests.get(point_url)
                                success = True
                            except requests.exceptions.RequestException as e:
                                print(f"Failed to fetch data for hosp_id {row_hosp['hosp_id']} with error: {e}")
                                print("Retrying...")
                                time.sleep(5)
                        
                        soup_point = BeautifulSoup(response_point.text, 'html.parser')
                        tables_point = soup_point.find_all('table')

                        # Choose specialist table
                        spc_table = tables_point[n_table]
                    
                        # Extract table data
                        rows = spc_table.find_all('tr')
                        spc = []
                        for row_value in rows:
                            columns_value = row_value.find_all(['td'])[0:7]
                            row_data_value = [column_value.get_text(strip=True) for column_value in columns_value]
                            spc.append(row_data_value)
                    
                        # Convert the scraped data into a DataFrame
                        spc_hos_df = pd.DataFrame(spc)

                        # Assign the first row as the header
                        spc_hos_df.columns = spc_hos_df.iloc[0]
                        spc_hos_df = spc_hos_df[1:]  
                        spc_hos_df = spc_hos_df.loc[~(spc_hos_df.iloc[:, 0].eq('') | spc_hos_df.iloc[:, 0].eq('ลำดับ'))]
                        
                        # Check if the hospital is 'public' or 'private' from number of column
                        if row_link['link'] == 'infopersonal':
                            if spc_hos_df.columns.str.contains('ข้าราชการ').any():
                                spc_hos_df = spc_hos_df.iloc[0:1, 1:6]
                                spc_hos_df.iloc[:, 1:5] = spc_hos_df.iloc[:, 1:5].apply(pd.to_numeric, errors='coerce')
                                spc_hos_df['total'] = spc_hos_df.iloc[:, 1:5].apply(lambda row: row.sum() if not row.isnull().all() else None, axis=1)
                                spc_hos_df = spc_hos_df.drop(spc_hos_df.columns[1:5], axis=1)
                            else:
                                spc_hos_df = spc_hos_df.iloc[0:1, 1:3]
                                spc_hos_df.rename(columns=lambda x: x.replace('FT', 'total') if x.startswith('FT') else x, inplace=True)
                                spc_hos_df['total'] = spc_hos_df['total'].apply(
                                    lambda x: int(x) if pd.notnull(x) and x != '' else pd.NA)
                        else:
                            if len(spc_hos_df.columns) == 4:
                                spc_hos_df = spc_hos_df.iloc[:, 1:3]
                                spc_hos_df.rename(columns=lambda x: x.replace('Full Time', 'total') if x.startswith('Full Time') else x, inplace=True)
                                spc_hos_df['total'] = spc_hos_df['total'].apply(
                                    lambda x: int(x) if pd.notnull(x) and x != '' else pd.NA)
                            elif len(spc_hos_df.columns) == 7:
                                spc_hos_df = spc_hos_df.iloc[:, 1:6]
                                spc_hos_df.iloc[:, 1:6] = spc_hos_df.iloc[:, 1:6].apply(pd.to_numeric, errors='coerce')
                                spc_hos_df['total'] = spc_hos_df.iloc[:, 1:6].apply(lambda row: row.sum() if not row.isnull().all() else None, axis=1)
                                spc_hos_df = spc_hos_df.drop(spc_hos_df.columns[1:5], axis=1)
                        
                        spc_hos_df = spc_hos_df.rename(columns={spc_hos_df.columns[0]: 'spc'})        
                        spc_hos_df.insert(0, 'hosp_id', row_hosp['hosp_id'])
                        spc_hos_df['time'] = time_input
                        spc_df = pd.concat([spc_df, spc_hos_df], ignore_index=True)
            
        
            P = (N/len(hosp_id))*100

            # Update progress bar and save partial results
            pbar.set_description(f"Progress {N}/{len(hosp_id)} hcode = {row_hosp['hosp_id']}")
            N = N+1
            if N % 100 == 0:
                partial_file_path = f'{output_folder}/spc_latest.csv'
                if os.path.exists(partial_file_path):
                    os.remove(partial_file_path)
                spc_df.to_csv(partial_file_path, index=False)

            # Update progress bar
            pbar.update(1)

    # Delete the partial file before saving the final CSV
    partial_file_path = os.path.join(output_folder, 'spc_latest.csv')
    if os.path.exists(partial_file_path):
        os.remove(partial_file_path)

    # Save final results
    final_file_path = os.path.join(output_folder, 'spc.csv')
    spc_df.to_csv(final_file_path, index=False)
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Update README
    readme_path = os.path.join(output_folder, 'readme.txt')
    with open(readme_path, 'w') as readme_file:
        readme_file.write(f"\n---------------------------------------\n")
        readme_file.write(f"Specialty Data Scraping\n")
        readme_file.write(f"Data Source: http://gishealth.moph.go.th/healthmap/\n")
        readme_file.write(f"Last Updated: {timestamp}\n")
        readme_file.write(f"\n---------------------------------------\n")
        readme_file.write(f"Data have been scraped from multiple hospitals and processed into a clean format.\n")
        readme_file.write(f"The resulting CSV file ('spc.csv') contains data on medical specialties by hospital.\n")

    print(f"Scraped data have been successfully saved to '{final_file_path}'\n(Last updated: {timestamp}).")
    
    return final_file_path

# Example usage in Jupyter Notebook:
if __name__ == "__main__":
    # Example DataFrames for hosp_id and spc_link
    hosp_id_example = pd.DataFrame({'hosp_id': ['001', '002']})
    spc_link_example = pd.DataFrame({'link': ['infopersonal', 'infohospital']})
    output_dir = "./output"
    
    # Run the scraper
    output_file = scrap(hosp_id_example, spc_link_example, output_dir)
    print(f"Scraped data is available in: {output_file}")