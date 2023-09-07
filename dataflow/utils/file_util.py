from pathlib import Path
import logging
import json
import io
import pandas as pd

def get_project_root() -> Path:
    return Path(__file__).parent.parent

def read_json_file(path: str) -> dict:
    root = get_project_root()
    #path_to_file = '{}\{}'.format(root, path)
    path_to_file = '{}/{}'.format(root, path)
    logging.info(f'Reading file >> {path_to_file}')
    with open(path_to_file) as json_file:
        data = json.load(json_file)
    return data

def process_csv(sep, element):
    if sep is None:
        sep = ','
    df = None
    try:
        df = pd.read_csv(io.BytesIO(element), sep=sep, engine='python')
    except Exception as e:
        logging.info(f"Found exception while decoding.. going for another decoding")
        df = pd.read_csv(io.BytesIO(element), sep=sep, engine='python', encoding='unicode_escape')
        logging.info(f"Using unicode_escape encoding")
    data = df.to_json(orient='records')
    op_data = json.loads(data)
    return op_data
