import pandas as pd
import re
import sqlite3
import csv
import json
import dicttoxml
from lxml import etree


def create_xml(file_name):
    with open(file_name, 'r') as json_file:
        obj = json.load(json_file)
    xml = dicttoxml.dicttoxml(obj, attr_type=False, root='convoy', item_func=lambda x: 'vehicle')
    file_name = re.sub(r'\.json$', '.xml', filename)
    root = etree.fromstring(xml)
    tree = etree.ElementTree(root)
    tree.write(file_name)
    if len(root) == 1:
        print(f'1 vehicle was saved into {file_name}')
    else:
        print(f'{len(root)} vehicles were saved into {file_name}')


def create_json(file_name, correct_df):
    convoy_dict = correct_df.to_dict(orient='records')
    with open(file_name, 'w') as out_file:
        json.dump({'convoy': convoy_dict}, out_file)
        if len(convoy_dict) == 1:
            print(f'1 vehicle was saved into {filename}')
        else:
            print(f'{len(convoy_dict)} vehicles were saved into {filename}')


def create_database(file_name):
    conn = sqlite3.connect(file_name)
    cursor = conn.cursor()
    # create convoy table if it doesn't exist
    cursor.execute('CREATE TABLE IF NOT EXISTS convoy(vehicle_id INT PRIMARY KEY, '
                   'engine_capacity INT NOT NULL, '
                   'fuel_consumption INT NOT NULL, '
                   'maximum_load INT NOT NULL)')
    # insert and count records added to table
    record_count = 0
    for line in buffer:
        try:
            cursor.execute(f'INSERT INTO convoy VALUES({line[0]}, {line[1]}, {line[2]}, {line[3]})')
        except Exception:
            cursor.execute(f'REPLACE INTO convoy VALUES({line[0]}, {line[1]}, {line[2]}, {line[3]})')
        record_count += 1
    conn.commit()
    conn.close()
    # check and print database entries
    if record_count == 1:
        print(f'1 record was inserted into {filename}')
    else:
        print(f'{record_count} records were inserted into {filename}')


def create_buffer(file_name):
    with open(file_name, newline='') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        data_from_csv = list()
        for record in csv_reader:
            if line_count == 0:
                line_count += 1
            else:
                data_from_csv.append(record)
                line_count += 1
    return data_from_csv


def convert_dirty_xlsx_to_csv(file_name):
    df = pd.read_excel(file_name, sheet_name='Vehicles', dtype=str)  # create dataframe from dirty file
    file_name = re.sub(r'\.xlsx$', '', file_name) + '.csv'  # change filename extension to .csv
    df.to_csv(file_name, index=False)
    lines = df.shape[0]  # calculate number of records added to dirty .csv file
    if lines == 1:
        print(f'1 line was added to {file_name}')
    else:
        print(f'{lines} lines were imported to {file_name}')
    return file_name, df


def clean_dirty_csv(file_name, df):
    file_name = re.sub(r'\.csv$', '[CHECKED].csv', file_name)  # add [CHECKED] tag to filename
    correct_df = df.replace(to_replace=r'\D', value='', regex=True)  # clean data from non-numeric values
    correct_df.to_csv(file_name, index=False)  # save clean data to [CHECKED].csv file
    comparison_df = correct_df.compare(df, keep_shape=True)  # create comparison dataframe to check for differences
    diff_count = 0

    # iterate cell by cell checking for not null cells
    for i in range(comparison_df.shape[0]):
        for j in range(comparison_df.shape[1]):
            if pd.isnull(comparison_df).iloc[i][j]:
                pass  # if cell is null no difference
            else:
                diff_count += 0.5  # differences appear in pairs so one diff = 2 cells

    corrected_cells = int(diff_count)  # number of corrected cells
    if corrected_cells == 0:
        pass
    elif corrected_cells == 1:
        print(f'1 cell was corrected in {file_name}')
    else:
        print(f'{corrected_cells} cells were corrected in {file_name}')

    buff = create_buffer(file_name)
    file_name = re.sub(r'\[CHECKED].csv$', '.s3db', file_name)

    return file_name, correct_df, buff


filename_input = input('Input file name\n')

# handle .xlsx dirty file
if filename_input.endswith('.xlsx'):
    filename, dirty_df = convert_dirty_xlsx_to_csv(filename_input)
    filename, clean_df, buffer = clean_dirty_csv(filename, dirty_df)
    create_database(filename)
    filename = re.sub(r'\.s3db$', '.json', filename)
    create_json(filename, clean_df)
    create_xml(filename)

# handle .csv clean file
elif filename_input.endswith('[CHECKED].csv'):
    clean_df = pd.read_csv(filename_input)
    buffer = create_buffer(filename_input)
    filename = re.sub(r'\[CHECKED].csv$', '.s3db', filename_input)
    create_database(filename)
    filename = re.sub(r'\.s3db$', '.json', filename)
    create_json(filename, clean_df)
    create_xml(filename)

# handle .csv dirty file
elif filename_input.endswith('.csv'):
    dirty_df = pd.read_csv(filename_input)
    filename, clean_df, buffer = clean_dirty_csv(filename_input, dirty_df)
    create_database(filename)
    filename = re.sub(r'\.s3db$', '.json', filename)
    create_json(filename, clean_df)
    create_xml(filename)

# handle .s3db file
elif filename_input.endswith('.s3db'):
    conx = sqlite3.connect(filename_input)
    clean_df = pd.read_sql_query("SELECT * FROM convoy", conx)
    filename = re.sub(r'\.s3db$', '.json', filename_input)
    create_json(filename, clean_df)
    create_xml(filename)

#handle .json file
elif filename_input.endswith('.json'):
    create_xml(filename_input)





