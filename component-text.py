import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime

# Load the XML from the specified file path
tree = ET.parse('G:/samanth473_drive/CDP/CDA-phcaserpt-1.3.0-CDA-phcaserpt-1.3.1/examples/samples/CDAR2_IG_PHCASERPT_R2_STU3.1_SAMPLE.xml')
root = tree.getroot()

# Define the namespace map
namespaces = {
    'voc': 'http://www.lantanagroup.com/voc',
    'hl7': 'urn:hl7-org:v3',
    'sdtc': 'urn:hl7-org:sdtc',
    'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
}

# Get the current date and time
current_time = datetime.now().strftime("%Y%m%d%H%M%S")

# Function to extract HTML table data
def extract_html_tables(section, table_title, table_index):
    html_tables = section.findall('.//hl7:text//hl7:table', namespaces)
    html_table_files = []

    for html_table_idx, html_table in enumerate(html_tables, start=1):
        table_data = []
        headers = [th.text for th in html_table.findall('.//hl7:thead//hl7:tr//hl7:th', namespaces)]
        num_columns = len(headers) + 2  # +2 for HTML_TABLE_ID and TABLE_TITLE
        for row in html_table.findall('.//hl7:tbody//hl7:tr', namespaces):
            row_data = [td.text for td in row.findall('hl7:td', namespaces)]
            while len(row_data) < num_columns - 2:  # Ensure row has the correct number of columns
                row_data.append(None)
            row_data.append(table_index)  # Add identifier to link with main table
            row_data.append(table_title)  # Add table title to link with component title
            table_data.append(row_data)
        
        # Check if table_data has the same number of columns as headers
        if any(len(row) != num_columns for row in table_data):
            print(f"Skipping table with inconsistent column counts in component {table_index}")
            continue

        df_html_table = pd.DataFrame(table_data, columns=headers + ['HTML_TABLE_ID', 'TABLE_TITLE'])
        csv_file_path_html = f'G:/samanth473_drive/Parse/Result/CDA_html_table_{table_index}_{html_table_idx}_{current_time}.csv'
        df_html_table.to_csv(csv_file_path_html, index=False)
        html_table_files.append(csv_file_path_html)
    
    return html_table_files

# Extract all component sections
components = root.findall('.//hl7:structuredBody/hl7:component', namespaces)

# Process each component section
html_table_files = []
for component_index, component in enumerate(components, start=1):
    section = component.find('.//hl7:section', namespaces)
    if section is not None:
        # Extract title
        title = section.find('.//hl7:title', namespaces)
        component_title = title.text if title is not None else "Unknown"

        # Extract HTML tables
        html_files = extract_html_tables(section, component_title, component_index)
        html_table_files.extend(html_files)

# Display the list of saved HTML table files
print("Saved HTML table files:")
for file_path in html_table_files:
    print(file_path)
