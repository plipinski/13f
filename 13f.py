import pandas as pd
import requests
import re
import json
from pathlib import Path

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

def retrieve_data_from_url(url):
    response = requests.get(url, headers=headers)
    content = response.text

    # Find all <table>...</table> blocks, skipping the first one
    table_blocks = re.findall(r'<TABLE>(?:(?!</TABLE>).)*?Column(?:(?!</TABLE>).)*?</TABLE>', content, re.DOTALL | re.IGNORECASE)

    if not table_blocks:
        return pd.DataFrame()

    # Process each table block
    processed_rows = []
    for i, table in enumerate(table_blocks):
        lines = table.strip().splitlines()
        lines = remove_until_s(lines)
        # Skip the first 9 lines and the last 3 (or 5 if last table)
        if i == len(table_blocks) - 1:
            trimmed_lines = lines[:-6]
        else:
            trimmed_lines = lines[:-4]
        processed_rows.extend(trimmed_lines)


    block = table_blocks[0]
    positions = []
    for match in re.finditer(r"<[SC]>", block):
        positions.append(match.start())

    # Sort and compute distances (i.e., column widths)
    positions.sort()
    column_widths = [positions[i + 1] - positions[i] for i in range(len(positions) - 1)]


    # Parse all lines into a dataframe
    parsed_data = [parse_fixed_width(row, column_widths) for row in processed_rows]
    df = pd.DataFrame(parsed_data)

    # Merge rows where first column is non-empty and second is empty
    merged_rows = []
    i = 0
    while i < len(df):
        row = df.iloc[i].copy()
        while row[0] and not row[1] and i + 1 < len(df):
            i += 1
            next_row = df.iloc[i]
            row[0] += ' ' + next_row[0]
            row[1:] = next_row[1:]
        merged_rows.append(row)
        i += 1

    df = pd.DataFrame(merged_rows)

    # Fill down first three columns if first column is empty
    for i in range(1, len(df)):
        if not df.iloc[i, 0]:
            df.iloc[i, 0:3] = df.iloc[i-1, 0:3]

    return df

# Convert specified columns to integers
#int_columns = [3, 4, 9, 10, 11]
#for col in int_columns:
#    df[col] = df[col].replace('', '0').str.replace(',', '').astype(int)


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

for url in txt_links:
    data = retrieve_data_from_url(url[1])
    if not data.empty:
        print(url[0] + '    ' + url[1])
        print(data)



