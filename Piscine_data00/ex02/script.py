import psycopg2
import sys, os

if (len(sys.argv) != 2):
    print("nop")
    exit(1)

if (not sys.argv[1].endswith(".csv")):
    print("nop pas csv")
    exit(1)

try:
    file = open(sys.argv[1], 'r')
except IOError:
    print("file not found")
    exit(1)

file.close()

namefile = os.path.basename(sys.argv[1])
nametable = namefile[:len(namefile) - 4]

conn = psycopg2.connect(database="postgres",
                        user='postgres', password='mysecretpassword', 
                        host='127.0.0.1', port='5432'
)

conn.autocommit = True
cursor = conn.cursor()

sql = f'''CREATE TABLE IF NOT EXISTS {nametable} (
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
           COPY {nametable} FROM stdin WITH CSV HEADER
           DELIMITER as ','
           """

with open(sys.argv[1], 'r') as f:
    cur.copy_expert(sql=copy_sql, file=f)
conn.commit()
conn.close()
