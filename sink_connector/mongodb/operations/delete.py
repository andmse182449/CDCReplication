import json
from psycopg2 import sql
from mongodb.operations.base_operation import BaseOperation

class Delete(BaseOperation):
    def execute(self, change_stream_document):
        document_key = self.get_document_key(change_stream_document)
        document_key['_id'] = json.loads(document_key['_id'])["$oid"]

        print(f"Deleting record with _id: {document_key['_id']}")

        query = sql.SQL("DELETE FROM {} WHERE _id = %s").format(
            sql.Identifier(self.destination_table)
        )
        self.cursor.execute(query, [document_key['_id']])
        self.conn.commit()