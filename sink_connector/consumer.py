import time
from confluent_kafka import Consumer, TopicPartition
import json
from mongodb.operation_handler import OperationHandler
from config.config_file import ConfigExtractor
from offset import offset_manager

# def create_new_column(conn, table_name, column_name, data_type):
#     cursor = conn.cursor()
#     query = sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}").format(
#         sql.Identifier(table_name),
#         sql.Identifier(column_name),
#         sql.SQL(data_type)
#     )
#     cursor.execute(query)
#     conn.commit()
#     cursor.close()


#     # create_new_columns = mapping.get('evolve.columns', False)
#     # if create_new_columns:
#     #     for field in synced_data.keys():
#     #         if field not in colnames:
#     #             data_type = 'TEXT'
#     #             if isinstance(synced_data[field], int):
#     #                 data_type = 'INTEGER'
#     #             elif isinstance(synced_data[field], float):
#     #                 data_type = 'FLOAT'
#     #
#     #             create_new_column(conn, destination_table, field, data_type)
#     #             colnames.append(field)
#     #             coltypes[field] = psycopg2.extensions.new_type((0,), field.upper(), lambda x, y: x)
#

def consumer(mapping_file_path):
    consumer_conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'consumer',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(consumer_conf)

    extractor = ConfigExtractor(mapping_file_path)  # Extract all config options
    postgres_conn = extractor.get_db_connection()  # Get DB connection
    consumer.subscribe([extractor.get_topic()])  # Subscribe to topic
    all_config = extractor.get_config()  # Get all raw config

    while not consumer.assignment():
        consumer.poll(1)

    assigned_partitions = consumer.assignment()

    for partition in assigned_partitions:
        last_offset = offset_manager.read_last_offset(partition.partition)
        if last_offset is not None:
            consumer.seek(TopicPartition(partition.topic, partition.partition, last_offset))
        else:
            consumer.seek(TopicPartition(partition.topic, partition.partition, 0))

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            value = json.loads(msg.value().decode('utf-8'))

            OperationHandler(postgres_conn, all_config).handle_operation(value)

            # offset_manager.store_last_offset(msg.partition(), msg.offset())
            print(f"Stored offset: {msg.offset()} for partition {msg.partition()}")

            print("----------------------------------------------------------------------------------------")

            consumer.commit()

    except KeyboardInterrupt:
        print("Consumer interrupted by user")
    finally:
        consumer.close()
        postgres_conn.close()


if __name__ == '__main__':
    mapping_file_path = 'config.json'
    consumer(mapping_file_path)


