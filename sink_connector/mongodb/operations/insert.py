from psycopg2 import sql
from mongodb.operations.base_operation import BaseOperation

class Insert(BaseOperation):
    def execute(self, change_stream_document):
        full_document = self.get_full_document(change_stream_document)

        synced_data = self.sync_data(self.mapping ,full_document)

        # synced_data = self.convert_numpy_types(synced_data)

        colnames, coltypes = self.get_column_info()

        fields = [field for field in colnames if field in synced_data]


        self.map_data_types(synced_data, coltypes, fields)


        insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(self.destination_table),
            sql.SQL(", ").join(map(sql.Identifier, fields)),
            sql.SQL(", ").join(sql.Placeholder() * len(fields))
        )
        self.cursor.execute(insert_query, [synced_data[field] for field in fields])
        print(f"Inserted record with _id: {synced_data['_id']}")
        self.conn.commit()

