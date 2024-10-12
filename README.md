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

For more detailed instructions, check out the [Getting Started](#gettingstarted) sections below.

## Getting Started

1. Clone the repository:
   ```bash
   git clone https://github.com/andmse182449/CDCReplication.git
2. Navigate to the Directory: Open a terminal and navigate to the directory containing the Docker Compose file.
3. Run Docker Compose:
   ```bash
   docker-compose up -d
4. Accessing the Services:
Confluent Control Center is accessible at http://localhost:9021.
MongoDB UI is accessible on the port 8081.
5. Accessing MongoDB execution terminal:
   ```bash
   mongosh mongodb://localhost:27017
   # IF THE NODE IS NOT PRIMARY, CHANGE THE PORT TO 27018 OR 27019 !!!!
6. Starting "Change Streams" for MongoDB (required Mongo v6.0 or above):
   ```bash
   # CREATE NEW DATABASE
   use inventory

   # CREATE NEW COLLECTION
   db.createCollection("myCollection");
   
   # ENABLE CHANGE STREAMS OPPTION
   db.runCommand({
   collMod: "myCollection",
   changeStreamPreAndPostImages: { enabled: true}
   })
   
   # CHECK IF CHANGE STREAMS IS ENABLED (Optional)
   db.getCollectionInfos({name: "myCollection"})
   
7. Using another terminal for viewing PostgreSQL :
   ```bash
   docker exec -it postgres psql -U replicauser -d destinationdb
 
