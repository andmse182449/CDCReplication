from urllib.parse import urlparse

def extract_db_details_from_uri(connection_uri):
    # Parse the URI
    parsed_uri = urlparse(connection_uri)

    # Extract components from the URI
    dbname = parsed_uri.path[1:]  # Skip the leading '/'
    user = parsed_uri.username
    password = parsed_uri.password
    host = parsed_uri.hostname
    port = parsed_uri.port

    return {
        "dbname": dbname,
        "user": user,
        "password": password,
        "host": host,
        "port": port
    }
config = {
    "name": "storage",
    "connection.uri": "postgresql://replicauser:replicapass@localhost:5432/destinationdb",
    "field": {
        "name": "ordername",
        "_id": "_id",
        "price": "amount",
        "quantity": "numberofproduct",
        "currency": "currency",
        "createdate": "createdate"
    },
    "topic": "final.inventory.inventory",
    "evolve.columns": False,
    "convert.data.type": True
}

# Extract database connection details from the URI
db_details = extract_db_details_from_uri(config["connection.uri"])

# Now you can use `db_details` wherever you need to use the connection parameters
print(db_details)
