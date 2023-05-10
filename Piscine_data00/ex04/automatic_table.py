import psycopg2
import os

conn = psycopg2.connect(database="postgres",
                        user='postgres', password='mysecretpassword', 
                        host='127.0.0.1', port='5432'
)

conn.autocommit = True
cursor = conn.cursor()

for filename in os.listdir(os.getcwd() + "/customer"):
   with open(os.path.join(os.getcwd() + "/customer/", filename), 'r') as f: # open in readonly mode
      # do your stuff

    sql = f'''CREATE TABLE IF NOT EXISTS {filename[:len(filename) - 4]} (
    event_time timestamp with time zone,
    event_type VARCHAR (50),
    product_id INTEGER,
    price MONEY,
    user_id numeric,
    user_session text
    );'''

    cursor.execute(sql)
    cur = conn.cursor()
    copy_sql = f"""
            COPY {filename[:len(filename) - 4]} FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """

    with open(os.getcwd() + "/customer/" + filename, 'r') as f:
        cur.copy_expert(sql=copy_sql, file=f)

conn.commit()
conn.close()