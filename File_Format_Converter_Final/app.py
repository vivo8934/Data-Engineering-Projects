import sys
import os
import glob
import json
import pandas as pd
from operator import itemgetter

def get_column_names(schemas, ds_name, sorting_key='column_position'):
    if ds_name not in schemas:
        raise KeyError(f"Dataset '{ds_name}' not found in schemas.")
    column_details = schemas[ds_name]
    if not all(sorting_key in col for col in column_details):
        raise ValueError(f"Some columns in dataset '{ds_name}' are missing the sorting key '{sorting_key}'.")
    columns = sorted(column_details, key=itemgetter(sorting_key))
    #columns = sorted(column_details, key=lambda col: col[sorting_key])
    return [col['column_name'] for col in columns]

def read_csv(file, schemas):
    if not os.path.exists(file):
        raise FileNotFoundError(f"CSV file '{file}' not found.")
    ds_name = os.path.basename(os.path.dirname(file))
    columns = get_column_names(schemas, ds_name)
    df = pd.read_csv(file, names=columns)
    return df

def to_json(df, tgt_base_dir, ds_name, file_name):
    output_dir = os.path.join(tgt_base_dir, ds_name)
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, file_name.replace('.csv', '.json'))
    df.to_json(output_file, orient='records', lines=True)

def file_converter(src_base_dir,tgt_base_dir,ds_name):
    #src_base_dir = 'data/retail_db'
    #tgt_base_dir = 'data/retail_json'
    schemas = json.load(open(f'{src_base_dir}/schemas.json'))
    files = glob.glob(f'{src_base_dir}/{ds_name}/part-*')
    if len(files) == 0:
        raise NameError('No Files Found for {ds_name}')
    for file in files:
        try:
            df = read_csv(file, schemas)
            file_name = os.path.basename(file)
            to_json(df, tgt_base_dir, ds_name, file_name)
        except Exception as e:
            print(f"[ERROR] Failed to process {file}: {e}")

def process_files(ds_names=None):
    src_base_dir = os.environ.get('SRC_BASE_DIR')
    tgt_base_dir = os.environ.get('TGT_BASE_DIR')

    schemas = json.load(open(f'{src_base_dir}/schemas.json'))
    if not ds_names:
        ds_names = schemas.keys()
    for ds_name in ds_names:
        try:
            print(f'Processing: {ds_name}')
            file_converter(src_base_dir,tgt_base_dir,ds_name)
        except NameError as ne:
            print(ne)
            print(f'Name Error proccessing {ds_name}')
            pass


if __name__ == '__main__':
    if len(sys.argv) == 2:
        ds_names = json.loads(sys.argv[1])
        process_files(ds_names)
    else:
        process_files()