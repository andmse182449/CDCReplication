# MongoDB to PostgreSQL CDC Pipeline

This project implements a **Change Data Capture (CDC)** pipeline that streams data from MongoDB to PostgreSQL. The pipeline captures inserts, updates, and deletions from MongoDB using change streams and synchronizes the changes to a PostgreSQL database.

## Overview

- **Source**: MongoDB (using change streams)
- **Stream Processing**: Kafka and Kafka Connect
- **Sink**: PostgreSQL
- **Data Transformation**: Python scripts for data transformation and syncing

## Features

- Real-time streaming of MongoDB change events.
- Custom data transformations between MongoDB and PostgreSQL schema.
- Seamless handling of inserts, updates, and deletes.
- Scalable and fault-tolerant architecture using Kafka.

## Setup and Usage

1. **Configure Kafka Connect**: Set up MongoDB Source Connector to capture change streams.
2. **Run the Python scripts**: These handle data transformation and sync between MongoDB and PostgreSQL.
3. **PostgreSQL sink**: Data is stored in PostgreSQL after transformation.

For more detailed instructions, check out the [Getting Started](#getting-started) sections below.

## Getting Started

1. Clone the repository:
   ```bash
   git clone https://github.com/andmse182449/CDCReplication.git
   ```
2. Navigate to the Directory: Open a terminal and navigate to the directory containing the Docker Compose file.
   ```bash
   cd CDCReplication
   ```
4. Run Docker Compose:
   ```bash
   docker-compose up -d
   ```
5. Accessing the Services:
Confluent Control Center is accessible at http://localhost:9021.
MongoDB UI is accessible on the port 8081.
6. Accessing MongoDB execution terminal:
   ```bash
   mongosh mongodb://localhost:27017
   ```
> [!IMPORTANT]
> If the Node is not in primary, change the port to 27018 or 27019.
6. Starting "Change Streams" for MongoDB (required Mongo v6.0 or above):
   ```
   # CREATE NEW DATABASE
   use inventory

   # CREATE NEW COLLECTION
   db.createCollection("myCollection")
   
   # ENABLE CHANGE STREAMS OPPTION
   db.runCommand({
   collMod: "myCollection",
   changeStreamPreAndPostImages: { enabled: true}
   })
   
   # CHECK IF CHANGE STREAMS IS ENABLED (Optional)
   db.getCollectionInfos({name: "myCollection"})
7. Create Kafka MongoDB connector for streaming changes:
   ```bash
   curl -X POST -H "Content-Type: application/json" --data @connectors/source-connector.json http://localhost:8083/connectors
   ```

   For deleting the connector:
   ```bash
   curl -X DELETE 'http://localhost:8083/connectors/source-connector'
   ```

   For viewing the status of the connector:
   ```bash
   curl -X GET 'http://localhost:8083/connectors/source-connector/status'
   ```

   You can check the file *source-connector.json* in the *connectors* folder to see full config for the connector. When opening the file, it would look like below:
   
   ```json
   {
     "name": "source-connector",
     "config": {
       "connector.class": "com.mongodb.kafka.connect.MongoSourceConnector",
       "connection.uri": "mongodb://mongo1:27017/?replicaSet=rs0",
       "database": "inventory",
       "collection": "myCollection",
       "topic.prefix": "consumer",
       "change.stream.full.document.before.change": "whenAvailable",
       "change.stream.full.document": "updateLookup",
       "publish.full.document.only": "false",
       "value.converter.schemas.enable": "false",
       "output.format.key": "schema",
       "output.format.value": "schema",
       "output.schema.infer.value": "true",
       "key.converter": "org.apache.kafka.connect.json.JsonConverter",
       "value.converter": "org.apache.kafka.connect.json.JsonConverter"
     }
   }
   ```
8. Inserting a document:
   ```
   db.myCollection.insertOne({
     "name":"10h",
     "quantity":10,
     "price":100,
     "currency":"USD",
     "createdate":Math.floor(Date.now()/1000)
   })
   ```
   Now, the message would pop up and could be seen in the *consumer.inventory.myCollection* topic. If you cannot see any topic being created, checking the log for any potential errors.
9. For consuming the messages and replicating data into PostgreSQL, we will now run a Python script to execute such action.
   
   First, accessing into *sink_connector* folder. Now, you have to install all the libraries needed for the Python Consumer.
   ```bash
   python -m pip install -r requirements.txt
   ```

   Next, find the file *config.json* where all the custom-built configs lie. Each option will be explained below:
   ```json
   {
     "table" : "storage", 
     "connection.uri" : "postgresql://replicauser:replicapass@localhost:5432/destinationdb",
     "field": {
       "name": "ordername",
       "_id": "_id",
       "price": "amount",
       "quantity": "numberofproduct",
       "currency": "currency",
       "createdate": "createdate"
     },
     "topic": "consumer.inventory.inventory",
     "data.type.mapping" : true
   }
   ```

   ## Configuration Fields

- **table**: Name of the destination table in PostgreSQL.
- **connection.uri**: Connection URL of PostgreSQL.
- **field**: Mapping between fields in the MongoDB documents (*on the right column*) and the corresponding fields in the PostgreSQL table (*on the left column*).
- **topic**: Topic from which the script consumes messages.
- **data.type.mapping**: When set to true, any conflicts between data types from MongoDB and PostgreSQL can be handled.

   
   When done installing all the required libraries, you can now run the script to start replicating the data from MongoDB into PostgreSQL.
   ```bash
   python consumer.py
   ```
 10. Accessing PostgreSQL execution terminal to see the resultl.
   ```
   docker exec -it postgres psql -U replicauser -d destinationdb

   select * from storage;
   ```
