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

For more detailed instructions, check out the [Installation](#installation) and [Configuration](#configuration) sections below.

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/your-repo.git
