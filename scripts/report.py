import argparse
from helpers import load_config, get_database_config
from logzero import logger
from query_executor import Executor

def main():
    parser = argparse.ArgumentParser(description='Retrieve database configuration for a client and database.')
    parser.add_argument('--client', help='Name of the client', required=True)
    parser.add_argument('--db_name', help='Name of the database', required=True)

    args = parser.parse_args()

    config = load_config('../db_config/config.yaml')

    database_config = get_database_config(args.client, args.db_name, config)

    if database_config:
        logger.info(f"Database Configuration for {args.client} - {args.db_name}:")
        logger.info(f"Host: {database_config['host']}")
        logger.info(f"Port: {database_config['port']}")
        logger.info(f"Username: {database_config['user']}")
        
    else:
        logger.error(f"Database not found for {args.client} - {args.db_name}")
    
    e = Executor(database_config)
    e.get_top_10_most_time_taken_queries()
    e.get_schema_information()
    e.get_triggers_information()
    e.get_procedures_information()
    e.get_columns_with_blob_data_type()
    e.get_indexes_for_tables()
    e.get_info_if_referenced_key_is_indexed()



if __name__ == "__main__":
    main()
