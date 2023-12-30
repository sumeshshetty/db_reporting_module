import yaml
import pymysql
from logzero import logging

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
