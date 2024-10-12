from mongodb.operations.insert import Insert
from mongodb.operations.update import Update
from mongodb.operations.delete import Delete

class OperationHandler:
    def __init__(self, conn, mapping):
        self.conn = conn
        self.mapping = mapping

    def handle_operation(self, change_stream_document):
        operations = {
            "insert": Insert,
            "update": Update,
            "delete": Delete,
        }
        operation_type = change_stream_document.get("operationType")
        if operation_type in operations:
            operations[operation_type](self.conn, self.mapping).execute(change_stream_document)
        else:
            raise ValueError(f"Unknown operation type: {operation_type}")
