import yaml
import pymysql
from logzero import logging
import pandas as pd
import os

def load_config(file_path):
    with open(file_path, 'r') as config_file:
        config_data = yaml.safe_load(config_file)
    return config_data

def get_database_config(client_name, database_name, config):
    for client in config.get('clients', []):
        if client['name'] == client_name:
            for db in client.get('databases', []):
                if db['database'] == database_name:
                    return db
    return None

def connection(db_config):
    # logging.info("initialized connection..")
    print("initialized connection..")
    return pymysql.connect(**db_config)

def close_connection(connection):
    connection.close()
    # logging.info("connection closed...")
    print("connection closed...")


def load_queries(file_path):
    with open(file_path, 'r') as file:
        queries = yaml.safe_load(file)
    return queries

def df_to_csv(data, cursor,folder, filename ):
    folder = os.path.join('..', folder)
    os.makedirs(folder, exist_ok=True)  # Create the 'output' folder if it doesn't exist
    csv_file_path = os.path.join(folder, filename)
            
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(data, columns=columns)
    df.to_csv(csv_file_path, index=False)

    print(f'DataFrame saved to {csv_file_path}')
    