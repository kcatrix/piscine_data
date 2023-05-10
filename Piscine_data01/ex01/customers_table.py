import psycopg2
import os

conn = psycopg2.connect(database="postgres",
                        user='postgres', password='mysecretpassword', 
                        host='127.0.0.1', port='5432'
)

conn.autocommit = True
cursor = conn.cursor()

# Create a table called "clients"
cursor.execute('''CREATE TABLE IF NOT EXISTS clients (
    event_time timestamp with time zone,
    event_type VARCHAR (50),
    product_id INTEGER,
    price MONEY,
    user_id numeric,
    user_session text
    );''')

# Loop through all CSV files in the customer directory that match data_202*_
for filename in os.listdir(os.getcwd() + "/customer"):
    if filename.startswith("data_202") and filename.endswith(".csv"):
        # Open each file and copy its contents into the "clients" table
        with open(os.path.join(os.getcwd() + "/customer/", filename), 'r') as f:
            cur = conn.cursor()
            copy_sql = f"""
                COPY clients FROM stdin WITH CSV HEADER
                DELIMITER as ','
                """

            cur.copy_expert(sql=copy_sql, file=f)

conn.commit()
conn.close()

conn.close()