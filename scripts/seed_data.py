import random
from faker import Faker
import psycopg2
from datetime import datetime


fake = Faker()

conn = psycopg2.connect(
    host='localhost',
    port=5433,
    dbname='sales_source',
    user='admin',
    password='admin'
)


cur = conn.cursor()

# insert customers

for _ in range(50):
    name = fake.name()
    email = fake.email()
    city = fake.city()
    updated_at = datetime.now()
    cur.execute('''INSERT INTO customers (name, email, city, updated_at)
     VALUES (%s, %s, %s, %s)''', (name, email, city, updated_at))


# insert products

for _ in range(20):
    cur.execute(
        '''INSERT INTO products (product_name, category, price, updated_at)
         VALUES (%s, %s, %s, %s)''',(
             fake.words(),
             fake.word(),
             round(random.uniform(10.0, 100.0), 2),
             datetime.now()
         )
    )


# insert orders

for _ in range(100):
    cur.execute("""
        INSERT INTO orders (customer_id, product_id, quantity, total_amount, order_date, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        random.randint(1, 50),
        random.randint(1, 20),
        random.randint(1, 5),
        round(random.uniform(20, 1000), 2),
        datetime.now(),
        datetime.now()
    ))

conn.commit()
cur.close()
conn.close()

print("Data inserted successfully.")
