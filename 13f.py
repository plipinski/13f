import pandas as pd
import requests
import re
import json
from datetime import datetime
from pathlib import Path
import csv

cusip = {}

with open('cusip.csv', newline='') as f:
    reader = csv.reader(f)
    for row in reader:
        if len(row) >= 2:  # make sure there's at least two columns
            cusip[row[1]] = row[0]


def remove_until_s(lines):
    for i, line in enumerate(lines):
        if line.lstrip().startswith("<S>"):
            return lines[i+1:]  # skip the <S> line as well
    return []

# Function to parse a line with fixed-width columns
def parse_fixed_width(line, widths):
    cols = []
    idx = 0
    for width in widths:
        cols.append(line[idx:idx+width].strip())
        idx += width
    return cols

def get_all_txt_links():
    with open('index.json', 'r') as f:
        data = json.load(f)

    return [(filing['filedAt'], filing['linkToTxt']) for filing in data.get('filings', []) if 'linkToTxt' in filing]

def get_column_widths(block):
    positions = []
    for match in re.finditer(r"<[SC]>", block):
        positions.append(match.start())

    # Sort and compute distances (i.e., column widths)
    positions.sort()
    return [positions[i + 1] - positions[i] for i in range(len(positions) - 1)]

def get_company_name(cusip_id):
    if cusip_id in cusip:
        return int(float(cusip[cusip_id]))
    return ""


def retrieve_data_from_url(url, filing_date):
    response = requests.get(url, headers=headers)
    content = response.text
    
    # Find all <table>...</table> blocks, skipping the first one
    table_blocks = re.findall(r'<TABLE>(?:(?!</TABLE>).)*?CUSIP(?:(?!</TABLE>).)*?</TABLE>', content, re.DOTALL | re.IGNORECASE)

    if not table_blocks:
        return pd.DataFrame()

    data = pd.DataFrame()
    # Process each table block
    processed_rows = []
    for i, table in enumerate(table_blocks):
        lines = table.strip().splitlines()
        lines = remove_until_s(lines)
        for i, line in enumerate(lines):
            if '---' in line:
                lines = lines[:i]  # keep everything before this line
                break

        column_widths = get_column_widths(table)

        # Parse all lines into a dataframe
        parsed_data = [parse_fixed_width(row, column_widths) for row in lines]
        df = pd.DataFrame(parsed_data)
        data = pd.concat([data, df], ignore_index=True)

    for i in range(1, len(data)):
        if data.iloc[i, 2].strip() == "":
            data.iloc[i, 1] = data.iloc[i-1, 1]
            data.iloc[i, 2] = data.iloc[i-1, 2]

    mask = ~data.apply(lambda row: row.astype(str).str.contains('---|===')).any(axis=1) & data.iloc[:, 3].fillna('').astype(str).str.strip().ne('')
    data = data[mask]

    try:
        data = data.iloc[:, [1,2,3,4]]
        data['filingAt'] = filing_date;
        return data
    except:
        return pd.DataFrame()

# MAIN

pd.set_option('display.max_rows', None)     # Show all rows
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.width', None)        # Disable line wrapping
pd.set_option('display.max_colwidth', None) # Show full content in each cell

headers = {
    "User-Agent": "Your Name (your.email@example.com)",
    "Accept-Encoding": "gzip, deflate",
    "Host": "www.sec.gov"
}

txt_links = get_all_txt_links()

all_dfs = []
for url in txt_links:
    data = retrieve_data_from_url(url[1], url[0])
    if not data.empty:
        print(url[0] + '    ' + url[1])
        all_dfs.append(data)

merged_df = pd.concat(all_dfs, ignore_index=True)

for i in range(len(merged_df)):
    merged_df.at[i, 'company'] = get_company_name(merged_df.iloc[i][2])

merged_df.to_csv("all_data.csv", index=False)



