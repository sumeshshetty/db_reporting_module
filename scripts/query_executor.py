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

            

            
        close_connection(self.connection)
        
