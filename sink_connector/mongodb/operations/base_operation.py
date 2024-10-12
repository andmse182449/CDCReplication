import numpy as np
import json
from bson import json_util
import pandas as pd
from mongodb.transform import cast_data_type


class BaseOperation:
    def __init__(self, conn, mapping):
        self.conn = conn
        self.mapping = mapping
        self.cursor = conn.cursor()
        self.destination_table = mapping['table']

    def sync_data(self, mapping, data):
        simplified_json_string = json.dumps(data)
        simplified_json_string_parsed = json.loads(simplified_json_string, object_hook=json_util.object_hook)
        parsed_data = self.parse_complex_json(simplified_json_string_parsed)
        flat_source_data = pd.json_normalize(parsed_data)

        destination_data = {}
        field_mapping = mapping['field']

        for source_field, destination_field in field_mapping.items():
            if source_field in flat_source_data:
                value = flat_source_data[source_field]
                destination_data[destination_field] = value.values[0] if len(value) > 0 else None

       # Updated logic to handle MongoDB _id field
        if '_id' in flat_source_data:
            id_value = flat_source_data['_id'].values[0]
            if isinstance(id_value, dict) and '$oid' in id_value:
                destination_data['_id'] = id_value['$oid']
            elif isinstance(id_value, str):
                try:
                    id_dict = json.loads(id_value)
                    if isinstance(id_dict, dict) and '$oid' in id_dict:
                        destination_data['_id'] = id_dict['$oid']
                    else:
                        destination_data['_id'] = id_value
                except json.JSONDecodeError:
                    destination_data['_id'] = id_value
            else:
                destination_data['_id'] = str(id_value)
        elif '_id.$oid' in flat_source_data:
            destination_data['_id'] = flat_source_data['_id.$oid'].values[0] if len(
                flat_source_data['_id.$oid']) > 0 else None

        return destination_data

    def parse_complex_json(self, data):
        if isinstance(data, str):
            try:
                return json.loads(data, object_hook=json_util.object_hook)
            except json.JSONDecodeError:
                return data
        return data

    # def convert_numpy_types(self, data):
    #     return {
    #         key: (int(value) if isinstance(value, np.integer) else
    #               float(value) if isinstance(value, np.floating) else value)
    #         for key, value in data.items()
    #     }

    def get_column_info(self):
        # Fetch column names and types from the destination table
        self.cursor.execute(f"SELECT * FROM {self.destination_table} LIMIT 0")
        colnames = [desc[0] for desc in self.cursor.description]
        coltypes = {desc[0]: desc[1] for desc in self.cursor.description}
        return colnames, coltypes

    def map_data_types(self, data, coltypes, fields):
        if self.mapping.get('data.type.mapping', False):
            cast_data_type.cast(data, coltypes, fields)

    def get_document_key(self, change_stream_document):
        # Extract the document key from change stream document
        if "documentKey" not in change_stream_document:
            raise ValueError(f"Missing documentKey field: {change_stream_document}")
        return change_stream_document["documentKey"]

    def get_full_document(self, change_stream_document):
        # Extract the full document from change stream document
        if "fullDocument" not in change_stream_document:
            raise ValueError(f"Missing fullDocument field: {change_stream_document}")
        return change_stream_document["fullDocument"]

    def get_update_description(self, change_stream_document):
        # Extract the update description from change stream document
        if "updateDescription" not in change_stream_document:
            raise ValueError(f"Missing updateDescription field: {change_stream_document}")
        return change_stream_document["updateDescription"]
