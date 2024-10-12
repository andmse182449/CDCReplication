import json
from psycopg2 import sql
from mongodb.operations.base_operation import BaseOperation

class Update(BaseOperation):
    def execute(self, change_stream_document):
        update_description = self.get_update_description(change_stream_document)
        document_key = self.get_document_key(change_stream_document)

        # Parse ObjectId from string format
        document_key['_id'] = json.loads(document_key['_id'])["$oid"]

        updated_fields = update_description.get("updatedFields", {})
        removed_fields = update_description.get("removedFields", [])

        synced_data = self.sync_data(self.mapping, updated_fields)
        colnames, coltypes = self.get_column_info()

        fields = [field for field in colnames if field in synced_data]
        self.map_data_types(synced_data, coltypes, fields)

        update_fields = [f for f in fields if f != '_id']
        update_query = sql.SQL("UPDATE {} SET {} WHERE _id = %s").format(
            sql.Identifier(self.destination_table),
            sql.SQL(", ").join(
                sql.SQL("{} = %s").format(sql.Identifier(f)) for f in update_fields
            )
        )
        update_values = [synced_data[f] for f in update_fields] + [document_key['_id']]
        self.cursor.execute(update_query, update_values)

        if removed_fields:
            remove_query = sql.SQL("UPDATE {} SET {} WHERE _id = %s").format(
                sql.Identifier(self.destination_table),
                sql.SQL(", ").join(
                    sql.SQL("{} = NULL").format(sql.Identifier(f)) for f in removed_fields
                )
            )
            self.cursor.execute(remove_query, [document_key['_id']])

        print(f"Updated record with _id: {document_key['_id']}")
        self.conn.commit()
