def store_last_offset(partition, offset):
    with open(f'offset_{partition}.txt', 'w') as f:
        f.write(str(offset))

def read_last_offset(partition):
    try:
        with open(f'offset_{partition}.txt', 'r') as f:
            return int(f.read().strip())
    except FileNotFoundError:
        return None
