import pymysql

source_db_config = {
    'host': 'source_host',
    'user': 'source_user',
    'password': 'source_password',
    'db': 'source_db',
}

destination_db_config = {
    'host': 'destination_host',
    'user': 'destination_user',
    'password': 'destination_password',
    'db': 'destination_db',
}

table_name = 'your_table_name'

def get_rows(connection, table_name, start, limit):
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT * FROM {table_name} LIMIT {start}, {limit}")
        rows = cursor.fetchall()
    return rows

def insert_rows(connection, table_name, rows):
    if rows:
        columns = ', '.join('`'+str(i)+'`' for i in rows[0].keys())
        values_template = ', '.join(['%s']*len(rows[0]))
        with connection.cursor() as cursor:
            for row in rows:
                cursor.execute(f"INSERT INTO {table_name} ({columns}) VALUES ({values_template})", tuple(row.values()))
        connection.commit()

def main():
    start = 0
    limit = 250
    while True:
        print(f"Moving rows from {start} to {start+limit}")
        source_db = pymysql.connect(**source_db_config)
        destination_db = pymysql.connect(**destination_db_config)

        try:
            rows = get_rows(source_db, table_name, start, limit)
            if not rows:
                print("No more rows to move")
                break

            insert_rows(destination_db, table_name, rows)

            start += limit
        except Exception as e:
            print(f"An error occurred: {e}")
            break
        finally:
            source_db.close()
            destination_db.close()

main()
