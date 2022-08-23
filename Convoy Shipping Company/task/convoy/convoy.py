import pandas as pd
import re
import sqlite3
import json

def score_pitstop(engine_capacity, fuel_consumption):
    possible_km = 100 * engine_capacity / fuel_consumption
    if possible_km >= 450:
        return 2
    elif 2 * possible_km >= 450:
        return 1
    else:
        return 0

def score_fuel(fuel_consumption):
    total_fuel = 4.5 * fuel_consumption
    if total_fuel <= 230:
        return 2
    else:
        return 1

def score_capacity(maximum_load):
    if maximum_load >= 20:
        return 2
    else:
        return 0

def clean_xlsx(file_name):
    vehicles = pd.read_excel(file_name, sheet_name='Vehicles', dtype=str)
    clean_file_name = file_name.replace('.xlsx', '.csv')
    vehicles.to_csv(clean_file_name, index=False, header=True)
    if vehicles.shape[0] == 1:
        print('1 line was added to {}'.format(clean_file_name))
    else:
        print('{} lines were added to {}'.format(vehicles.shape[0], clean_file_name))

def clean_csv(file_name):
    vehicles = pd.read_csv(file_name, dtype=str)
    #column_names = list(vehicles.columns.values)
    cells_corrected = 0
    for col in vehicles:
        for i in range(vehicles.shape[0]):
            if re.search(r'\D+', vehicles[col][i]) is not None:
                cells_corrected += 1
                vehicles[col][i] = re.search(r'\d+', vehicles[col][i]).group()
    clean_file_name = file_name.replace('.csv', '[CHECKED].csv')
    vehicles.to_csv(clean_file_name, index=False, header=True)
    if cells_corrected == 1:
        print('1 cell was corrected in {}'.format(clean_file_name))
    else:
        print('{} cells were corrected in {}'.format(cells_corrected, clean_file_name))

def feed_db(file_name):
    convoy = pd.read_csv(file_name, dtype=int)
    for lab, row in convoy.iterrows():
        convoy.loc[lab, 'score'] = score_pitstop(row['engine_capacity'], row['fuel_consumption']) + score_fuel(row['fuel_consumption']) + score_capacity(row['maximum_load'])
    convoy.score = convoy.score.astype('int64')
    convoy = convoy.astype('str')
    db_name = file_name.replace('[CHECKED].csv', '.s3db')
    # create database
    conn = sqlite3.connect(db_name)
    my_cursor = conn.cursor()
    # create table
    query = 'CREATE TABLE convoy ('
    for col_name in convoy:
        if col_name == 'vehicle_id':
            query += col_name + ' INTEGER PRIMARY KEY, '
        else:
            query += col_name + ' INTEGER NOT NULL, '
    query_list = list(query)
    query_list[-2] = ')'
    del query_list[-1]
    query = ''.join(query_list)
    my_cursor.execute(query)
    # insert data
    records = convoy.to_records(index=False, column_dtypes=int).tolist()
    insert_data = 'INSERT INTO convoy VALUES ' + '(' + '?, ' * (convoy.shape[1] - 1) + '?)'
    my_cursor.executemany(insert_data, records)
    if len(records) == 1:
        print('1 record was inserted into {}'.format(db_name))
    else:
        print('{} records were inserted into {}'.format(len(records), db_name))
    # end
    conn.commit()
    my_cursor.close()
    conn.close()

def create_db(file_name):
    if file_name.endswith('.xlsx'):
        clean_xlsx(file_name)
        file_name = file_name.replace('.xlsx', '.csv')
    if file_name.endswith('.csv'):
        if not file_name.endswith('[CHECKED].csv'):
            clean_csv(file_name)
            file_name = file_name.replace('.csv', '[CHECKED].csv')
        if file_name.endswith('[CHECKED].csv'):
            feed_db(file_name)
            file_name = file_name.replace('[CHECKED].csv', '.s3db')

def export_json(file_name, df):
    dict_convoy = df.to_dict(orient='records')
    out = {'convoy': dict_convoy}
    name_json = file_name.replace('.s3db', '.json')
    with open(name_json, 'w') as json_file:
        json.dump(out, json_file)
    if len(dict_convoy) == 1:
        print('1 vehicle was saved into {}'.format(name_json))
    else:
        print('{} vehicles were saved into {}'.format(len(dict_convoy), name_json))

def export_xml(file_name, df):
    name_xml = file_name.replace('.s3db', '.xml')
    if df.shape[0] == 0:
        with open(name_xml, 'w') as f:
            f.write("<convoy></convoy>")
    else:
        df.to_xml(path_or_buffer=name_xml, index=False, root_name='convoy', row_name='vehicle', parser='etree', xml_declaration=False)
    if df.shape[0] == 1:
        print('1 vehicle was saved into {}'.format(name_xml))
    else:
        print('{} vehicles were saved into {}'.format(df.shape[0], name_xml))

def export_data(file_name):
    conn = sqlite3.connect(file_name)
    convoy = pd.read_sql_query('SELECT * FROM convoy', conn)
    conn.close()
    convoy_json = convoy[convoy['score'] > 3].drop(['score'], axis=1)
    convoy_xml = convoy[convoy['score'] <= 3].drop(['score'], axis=1)
    export_json(file_name, convoy_json)
    export_xml(file_name, convoy_xml)

def final(file_name):
    create_db(file_name)
    export_data(file_name)

file_name = input('Input file name\n')

if file_name.endswith('.xlsx'):
    clean_xlsx(file_name)
    file_name = file_name.replace('.xlsx', '.csv')
if file_name.endswith('.csv'):
    if not file_name.endswith('[CHECKED].csv'):
        clean_csv(file_name)
        file_name = file_name.replace('.csv', '[CHECKED].csv')
    if file_name.endswith('[CHECKED].csv'):
        feed_db(file_name)
        file_name = file_name.replace('[CHECKED].csv', '.s3db')
export_data(file_name)