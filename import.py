import pandas as pd
import sqlite3

# Load CSV file
users = pd.read_csv("users.csv")

events = pd.read_csv("events.csv")

sessions = pd.read_csv("sessions.csv")

billing = pd.read_csv("billing.csv")

# Extract multiple columns (e.g., "Name" and "Age")
subset = users[["user_id","signup_date","plan_tier","company_size","region","industry","acquisition_channel","is_enterprise","churned_30d","churned_90d","downgraded","expansion_event"]]


# 1. Connect to the database (or create it if it doesn't exist)
#    This will create a file named 'mydatabase.db' in the current directory.
conn = sqlite3.connect('mydatabase.db')

# 2. Create a cursor object to execute SQL commands
cursor = conn.cursor()

# 3. Define the SQL CREATE TABLE statement
#    This example creates a table named 'users' with 'id', 'name', and 'email' columns.



create_table_sql = """
DROP TABLE IF EXISTS user;
CREATE TABLE IF NOT EXISTS user (
    user_id TEXT PRIMARY KEY,
    signup_date TEXT,
    plan_tier TEXT,
    company_size TEXT,
    region TEXT,
    industry TEXT,
    acquisition_channel TEXT,
    is_enterprise INTEGER,
    churned_30d INTEGER,
    churned_90d INTEGER,
    downgraded INTEGER,
    expansion_event INTEGER
);
"""

# 4. Execute the SQL statement
cursor.executescript(create_table_sql)

# 5. Commit the changes to the database
conn.commit()

insert_sql = """
INSERT INTO user (
    user_id, signup_date, plan_tier, company_size, region, industry, acquisition_channel,
    is_enterprise, churned_30d, churned_90d, downgraded, expansion_event
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

for index, row in subset.iterrows():
    cursor.execute(insert_sql, (
        str(row['user_id']),
        str(row['signup_date']),
        str(row['plan_tier']),
        str(row['company_size']),
        str(row['region']),
        str(row['industry']),
        str(row['acquisition_channel']),
        int(row['is_enterprise']),
        int(row['churned_30d']),
        int(row['churned_90d']),
        int(row['downgraded']),
        int(row['expansion_event'])
    ))
    
df_preview = pd.read_sql_query("SELECT * FROM user LIMIT 10;", conn)
print(df_preview)

# 6. Close the database connection
conn.close()