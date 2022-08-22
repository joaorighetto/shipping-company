# Write your code here
import io
import pandas as pd
import re
import sqlite3
import csv
import json


filename_input = input('Input file name\n')

# this block reads the input file given onto a dataframe and, if necessary, converts .xlsx file to .csv
# outputting how many lines were imported
if filename_input.endswith('.xlsx'):
    df = pd.read_excel(filename_input, sheet_name='Vehicles', dtype=str)
    filename = re.sub(r'\.xlsx$', '', filename_input) + '.csv'
    df.to_csv(filename, index=False)
    lines = df.shape[0]
    columns = df.shape[1]
    if lines == 1:
        print(f'1 line was added to {filename}')
    else:
        print(f'{lines} lines were imported to {filename}')
else:
    df = pd.read_csv(filename_input)

# clean the dataframe of any non-numeric characters
correct_df = df.replace(to_replace=r'\D', value='', regex=True)

# add '[CHECKED].csv' to the filename if necessary
if filename_input.endswith('[CHECKED].csv') or filename_input.endswith('.s3db'):
    filename = filename_input
else:
    filename = re.sub(r'\.csv$|\.xlsx$', '', filename_input) + '[CHECKED].csv'

# import the corrected dataframe as '%filename%[CHECKED].csv'
correct_df.to_csv(filename, index=False)

# create comparison dataframe to check for differences
comparison_df = correct_df.compare(df, keep_shape=True)
diff_count = 0
for i in range(comparison_df.shape[0]):  # iterate over rows
    for j in range(comparison_df.shape[1]):  # iterate over columns
        if pd.isnull(comparison_df).iloc[i][j]:
            pass  # if cell is null do nothing
        else:
            diff_count += 1  # count cells comparing differences between df and correct_df

# calculate number of corrected cells and print it
corrected_cells = int(diff_count / 2)
if corrected_cells == 1:
    print(f'1 cell was corrected in {filename}')
elif corrected_cells == 0:
    pass
else:
    print(f'{corrected_cells} cells were corrected in {filename}')

# create buffer with the csv data for future insertion in database
with open(filename, newline='') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    data_from_csv = list()
    for line in csv_reader:
        if line_count == 0:
            header = line
            line_count += 1
        else:
            data_from_csv.append(line)

# convert filename to .s3db
if filename.endswith('[CHECKED].csv'):
    filename = re.sub(r'\[CHECKED].csv$', '.s3db', filename)

conn = sqlite3.connect(filename)
cursor = conn.cursor()
# create convoy table if it doesn't exist
cursor.execute('DROP TABLE IF EXISTS convoy')
cursor.execute('CREATE TABLE IF NOT EXISTS convoy(vehicle_id INT PRIMARY KEY, '
               'engine_capacity INT NOT NULL, '
               'fuel_consumption INT NOT NULL, '
               'maximum_load INT NOT NULL)')

# insert and count records added to table
record_count = 0
for line in data_from_csv:
    cursor.execute(f'INSERT INTO convoy VALUES({line[0]}, {line[1]}, {line[2]}, {line[3]})')
    record_count += 1

conn.commit()

# check and print database entries
if record_count == 1:
    print(f'1 record was inserted into {filename}')
else:
    print(f'{record_count} records were inserted into {filename}')

# convert filename to JSON
filename = re.sub(r'\.s3db$', '.json', filename)

# create dictionary from dataframe
convoy_dict = correct_df.to_dict(orient='records')

# create JSON file, check and print its entries
with open(filename, 'w') as out_file:
    json.dump({'convoy': convoy_dict}, out_file)
    if len(convoy_dict) == 1:
        print(f'1 vehicle was saved into {filename}')
    else:
        print(f'{len(convoy_dict)} vehicles were saved into {filename}')

conn.close()

