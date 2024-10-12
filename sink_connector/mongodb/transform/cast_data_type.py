from datetime import datetime, date
import uuid
import json
from dateutil import parser
import logging
import numpy as np

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def cast(synced_data, coltypes, fields):
    PG_TYPES = {
        'int2': 21, 'int4': 23, 'int8': 20,
        'float4': 700, 'float8': 701,
        'numeric': 1700,
        'bool': 16,
        'timestamp': 1114, 'timestamptz': 1184,
        'date': 1082,
        'time': 1083, 'timetz': 1266,
        'varchar': 1043, 'text': 25,
        'uuid': 2950,
        'json': 114, 'jsonb': 3802,
        'bytea': 17
    }

    for field in fields:
        if field in coltypes:
            target_type = coltypes[field]
            value = synced_data.get(field)

            if value is None:
                logger.debug(f"Skipping field {field} as value is None")
                continue

            try:
                # Handle NumPy types
                if isinstance(value, np.integer):
                    value = int(value)
                elif isinstance(value, np.floating):
                    value = float(value)
                elif isinstance(value, np.ndarray):
                    value = value.tolist()

                if target_type in (PG_TYPES['int2'], PG_TYPES['int4'], PG_TYPES['int8']):
                    synced_data[field] = int(value)
                elif target_type in (PG_TYPES['float4'], PG_TYPES['float8'], PG_TYPES['numeric']):
                    synced_data[field] = float(value)
                elif target_type == PG_TYPES['bool']:
                    synced_data[field] = bool(value)
                elif target_type in (PG_TYPES['timestamp'], PG_TYPES['timestamptz']):
                    if isinstance(value, (int, float)):
                        # Handle milliseconds timestamp
                        if value > 1e10:
                            value = value / 1000
                        synced_data[field] = datetime.fromtimestamp(value)
                    elif isinstance(value, str):
                        try:

                            # Try parsing as a unix timestamp first
                            timestamp = float(value)
                            if timestamp > 1e10:

                                timestamp = timestamp / 1000
                            synced_data[field] = datetime.fromtimestamp(timestamp)
                        except ValueError:
                            # If that fails, try parsing as an ISO format string
                            synced_data[field] = parser.parse(value)
                    else:
                        raise ValueError(f"Unexpected type for timestamp: {type(value)}")
                elif target_type == PG_TYPES['date']:
                    if isinstance(value, (int, float)):
                        synced_data[field] = date.fromtimestamp(value)
                    elif isinstance(value, str):
                        synced_data[field] = parser.parse(value).date()
                    else:
                        raise ValueError(f"Unexpected type for date: {type(value)}")
                elif target_type in (PG_TYPES['time'], PG_TYPES['timetz']):
                    if isinstance(value, str):
                        synced_data[field] = parser.parse(value).time()
                    else:
                        raise ValueError(f"Unexpected type for time: {type(value)}")
                elif target_type in (PG_TYPES['varchar'], PG_TYPES['text']):
                    synced_data[field] = str(value)
                elif target_type == PG_TYPES['uuid']:
                    synced_data[field] = uuid.UUID(str(value))
                elif target_type in (PG_TYPES['json'], PG_TYPES['jsonb']):
                    if isinstance(value, str):
                        synced_data[field] = json.loads(value)
                    elif isinstance(value, (dict, list)):
                        synced_data[field] = value
                    else:
                        raise ValueError(f"Unexpected type for json: {type(value)}")
                elif target_type == PG_TYPES['bytea']:
                    if isinstance(value, str):
                        synced_data[field] = value.encode('utf-8')
                    elif isinstance(value, bytes):
                        synced_data[field] = value
                    else:
                        raise ValueError(f"Unexpected type for bytea: {type(value)}")
                else:
                    logger.warning(f"Unhandled target type for field {field}: {target_type}")
            except (ValueError, TypeError, json.JSONDecodeError, OverflowError, OSError) as e:
                logger.error(f"Error converting {field} to {target_type}: {e}")
                logger.error(f"Problematic value: {value} (type: {type(value)})")
                # Keep the original value
                logger.warning(f"Keeping original value for field {field}")

    return synced_data