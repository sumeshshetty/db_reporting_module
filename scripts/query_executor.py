from helpers import connection, close_connection, load_queries, df_to_csv
import os
class Executor:
    def __init__(self, config):
        self.config = config
        self.connection = connection(self.config)
        self.queries = load_queries('../db_config/query_config.yaml')
    
    def get_top_10_most_time_taken_queries(self):
        
        with self.connection.cursor() as cursor:
            
            query = self.queries.get('get_top_10_most_time_taken_queries')
            query = query.replace(':schema', self.config['database'])
            
            cursor.execute(query)
            result = cursor.fetchall()
            
            df_to_csv(result, cursor,folder= 'reports'+'/'+self.config['database'], filename = 'top_10_most_time_taken_queries.csv')
            cursor.close()
            
        #close_connection(self.connection)
    
    def get_schema_information(self):
        with self.connection.cursor() as cursor:
            
            query = self.queries.get('get_schema_information')
            query = query.replace(':schema', self.config['database'])
            
            cursor.execute(query)
            result = cursor.fetchall()
            
            df_to_csv(result, cursor,folder= 'reports'+'/'+self.config['database'], filename = 'get_schema_information.csv')
            cursor.close()

        # close_connection(self.connection)
    
    def get_triggers_information(self):
        with self.connection.cursor() as cursor:
            
            query = self.queries.get('get_triggers_information')
            query = query.replace(':schema', self.config['database'])
            
            cursor.execute(query)
            result = cursor.fetchall()
            
            df_to_csv(result, cursor,folder= 'reports'+'/'+self.config['database'], filename = 'get_triggers_information.csv')
            cursor.close()
            
        # close_connection(self.connection)
    
    def get_procedures_information(self):
        with self.connection.cursor() as cursor:
            
            query = self.queries.get('get_procedures_information')
            query = query.replace(':schema', self.config['database'])
            
            cursor.execute(query)
            result = cursor.fetchall()
            
            df_to_csv(result, cursor,folder= 'reports'+'/'+self.config['database'], filename = 'get_procedures_information.csv')
            cursor.close()
            
        # close_connection(self.connection)
    
    def get_columns_with_blob_data_type(self):
        with self.connection.cursor() as cursor:
            
            query = self.queries.get('get_columns_with_blob_data_type')
            query = query.replace(':schema', self.config['database'])
            
            cursor.execute(query)
            result = cursor.fetchall()
            
            df_to_csv(result, cursor,folder= 'reports'+'/'+self.config['database'], filename = 'get_columns_with_blob_data_type.csv')
            cursor.close()
            
        # close_connection(self.connection)
    
    def get_indexes_for_tables(self):
        with self.connection.cursor() as cursor:
            
            query = self.queries.get('get_indexes_for_tables')
            query = query.replace(':schema', self.config['database'])
            
            cursor.execute(query)
            result = cursor.fetchall()
            
            df_to_csv(result, cursor,folder= 'reports'+'/'+self.config['database'], filename = 'get_indexes_for_tables.csv')
            cursor.close()
            
        # close_connection(self.connection)
    
    def get_info_if_referenced_key_is_indexed(self):
        with self.connection.cursor() as cursor:
            
            query = self.queries.get('get_info_if_referenced_key_is_indexed')
            query = query.replace(':schema', self.config['database'])
            
            cursor.execute(query)
            result = cursor.fetchall()
            
            df_to_csv(result, cursor,folder= 'reports'+'/'+self.config['database'], filename = 'get_info_if_referenced_key_is_indexed.csv')
            cursor.close()
            
        close_connection(self.connection)
    
    def check_if_no_index_present_at_all(self):
        with self.connection.cursor() as cursor:
            
            query = self.queries.get('check_if_no_index_present_at_all')
            query = query.replace(':schema', self.config['database'])
            
            cursor.execute(query)
            result = cursor.fetchall()
            
            df_to_csv(result, cursor,folder= 'reports'+'/'+self.config['database'], filename = 'check_if_no_index_present_at_all.csv')
            cursor.close()
            
        close_connection(self.connection)
    
    def check_foreign_key_is_present_but_index_is_not_there(self):
        with self.connection.cursor() as cursor:
            
            query = self.queries.get('check_foreign_key_is_present_but_index_is_not_there')
            query = query.replace(':schema', self.config['database'])
            
            cursor.execute(query)
            result = cursor.fetchall()
            
            df_to_csv(result, cursor,folder= 'reports'+'/'+self.config['database'], filename = 'check_foreign_key_is_present_but_index_is_not_there.csv')
            cursor.close()
            
        close_connection(self.connection)

    
        
