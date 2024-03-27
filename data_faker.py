from faker import Faker
import pymysql  # pymysql or any other database adapter you prefer
import random

# Create a Faker instance
fake = Faker()

# Database connection parameters
conn_params = {
    'host': '127.0.0.1',  # Replace with your host address
    'user': 'myuser',  # Replace with your username
    'password': 'mypassword',  # Replace with your password
    'db': 'mydatabase'  # Replace with your database name
}

# Establish a database connection
conn = pymysql.connect(**conn_params)
cur = conn.cursor()





# SQL for inserting data
insert_query = '''
INSERT INTO transactions (
    acc_code, cust_code, tx_type, tx_category, tx_amount, tx_currency, 
    tx_datetime, tx_post_balance, tx_reference, tx_channel, tx_status, fraud_flag
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

# Define some mock setup for your categorical fields
tx_types = ['IN', 'OU']
tx_categories = ['P', 'R']
tx_currencies = ['USD', 'EUR', 'GBP']
tx_channels = ['01', '02']
tx_statuses = ['A', 'P', 'R']

# Generate and insert 1 million rows of data
try:
    for _ in range(9000000):
        # Generate fake data matching the schema
        acc_code = fake.sha256()[:64]
        cust_code = fake.sha256()[:64]
        tx_type = random.choice(tx_types)
        tx_category = random.choice(tx_categories)
        tx_amount = round(random.uniform(10.00, 10000.00), 4)
        tx_currency = random.choice(tx_currencies)
        tx_datetime = fake.date_time_this_decade()
        tx_post_balance = round(random.uniform(100.00, 100000.00), 4)
        tx_reference = fake.sha256()[:64]
        tx_channel = random.choice(tx_channels)
        tx_status = random.choice(tx_statuses)
        fraud_flag = fake.boolean(chance_of_getting_true=2)  # 2% chance of being flagged as fraud
        
        # Insert data into the table
        cur.execute(insert_query, (
            acc_code, cust_code, tx_type, tx_category, tx_amount,
            tx_currency, tx_datetime, tx_post_balance, tx_reference,
            tx_channel, tx_status, fraud_flag
        ))
        
        # For better progress and memory management, commit every 10,000 rows
        if _ % 10000 == 0:
            print(f"Inserted {_} rows so far.")
            conn.commit()

    # Commit any remaining transactions
    conn.commit()
except Exception as e:
    print(f"An error occurred: {e}")
    conn.rollback()
    raise e  # Re-raise exception after rollback to acknowledge the failure
finally:
    # Close the cursor and the connection
    cur.close()
    conn.close()

print("Data generation complete!")
