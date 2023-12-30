from helpers import connection, close_connection
from logzero import logging
class Executor:
    def __init__(self, config):
        self.connection = connection(config)
    
    def get_top_10_most_time_taken_queries(self):
        
        with self.connection.cursor() as cursor:
            
            query = "SELECT version()"
            cursor.execute(query)
            result = cursor.fetchone()
            print(result)
        close_connection(self.connection)
        
