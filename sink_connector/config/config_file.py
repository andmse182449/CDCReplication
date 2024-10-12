import psycopg2
from urllib.parse import urlparse
import json


class ConfigExtractor:
    def __init__(self, file_path):
        with open(file_path, 'r') as file:
            config_data = json.load(file)

        self.config = config_data
        self.connection_uri = self.config.get('connection.uri')
        self.topic = self.config.get('topic')


    def get_topic(self):
        return self.topic

    def get_config(self):
        return self.config

    def get_db_connection(self):
        parsed_uri = urlparse(self.connection_uri )

        # Extract components from the URI
        dbname = parsed_uri.path[1:]
        user = parsed_uri.username
        password = parsed_uri.password
        host = parsed_uri.hostname
        port = parsed_uri.port

        return psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )


