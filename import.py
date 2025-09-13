import pandas as pd
import sqlite3


def getUsers():
    users = pd.read_csv("users.csv")
    # 1. Connect to the database (or create it if it doesn't exist)
    #    This will create a file named 'mydatabase.db' in the current directory.
    conn = sqlite3.connect('mydatabase.db')

    # 2. Create a cursor object to execute SQL commands
    cursor = conn.cursor()

    # 3. Define the SQL CREATE TABLE statement
    #    This example creates a table named 'users' with 'id', 'name', and 'email' columns.

    users = users[["user_id","signup_date","plan_tier","company_size","region","industry","acquisition_channel","is_enterprise","churned_30d","churned_90d","downgraded","expansion_event"]]


    create_table_sql = """
    DROP TABLE IF EXISTS users;
    CREATE TABLE IF NOT EXISTS users (
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
        session_ids TEXT
    );
    """

    # 4. Execute the SQL statement
    cursor.executescript(create_table_sql)

    # 5. Commit the changes to the database
    conn.commit()

    insert_sql = """
    INSERT INTO users (
        user_id, signup_date, plan_tier, company_size, region, industry, acquisition_channel,
        is_enterprise, churned_30d, churned_90d, downgraded, expansion_event
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    for index, row in users.iterrows():
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
        
    df_preview = pd.read_sql_query("SELECT * FROM users LIMIT 10;", conn)
    print(df_preview)

    # 6. Close the database connection
    conn.close()

def getEvents():
    events = pd.read_csv("events.csv")
    conn = sqlite3.connect('mydatabase.db')

    # 2. Create a cursor object to execute SQL commands
    cursor = conn.cursor()

    # 3. Define the SQL CREATE TABLE statement
    #    This example creates a table named 'users' with 'id', 'name', and 'email' columns.

    events = events[["event_id","user_id","session_id","ts","feature_name","action","duration_ms","latency_ms","success"]]


    create_table_sql = """
    DROP TABLE IF EXISTS events;
    CREATE TABLE IF NOT EXISTS events (
        event_id TEXT PRIMARY KEY,
        user_id TEXT,
        session_id TEXT,
        ts TEXT,
        feature_name TEXT,
        action TEXT,
        duration_ms INTEGER,
        latency_ms INTEGER,
        success INTEGER
    );
    """

    # 4. Execute the SQL statement
    cursor.executescript(create_table_sql)

    # 5. Commit the changes to the database
    conn.commit()

    insert_sql = """
    INSERT INTO events (
        event_id,user_id,session_id,ts,feature_name,action,duration_ms,latency_ms,success
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    for index, row in events.iterrows():
        cursor.execute(insert_sql, (
            str(row['event_id']),
            str(row['user_id']),
            str(row['session_id']),
            str(row['ts']),
            str(row['feature_name']),
            str(row['action']),
            int(row['duration_ms']),
            int(row['latency_ms']),
            int(row['success'])
        ))
        
    df_preview = pd.read_sql_query("SELECT * FROM events LIMIT 10;", conn)
    print(df_preview)

    conn.close()

def getSessions():
    sessions = pd.read_csv("sessions.csv")
    # 1. Connect to the database (or create it if it doesn't exist)
    conn = sqlite3.connect('mydatabase.db')

    # 2. Create a cursor object to execute SQL commands
    cursor = conn.cursor()

    # 3. Define the SQL CREATE TABLE statement
    sessions = sessions[["session_id","user_id","session_start","session_end","device","os","app_version","country","session_length_sec"]]

    create_table_sql = """
    DROP TABLE IF EXISTS sessions;
    CREATE TABLE IF NOT EXISTS sessions (
        session_id TEXT PRIMARY KEY,
        user_id TEXT,
        session_start TEXT,
        session_end TEXT,
        device TEXT,
        os TEXT,
        app_version TEXT,
        country TEXT,
        session_length_sec INTEGER
    );
    """

    # 4. Execute the SQL statement
    cursor.executescript(create_table_sql)

    # 5. Commit the changes to the database
    conn.commit()

    insert_sql = """
    INSERT INTO sessions (
        session_id, user_id, session_start, session_end, device, os, app_version, country, session_length_sec
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    for index, row in sessions.iterrows():
        cursor.execute(insert_sql, (
            str(row['session_id']),
            str(row['user_id']),
            str(row['session_start']),
            str(row['session_end']),
            str(row['device']),
            str(row['os']),
            str(row['app_version']),
            str(row['country']),
            int(row['session_length_sec'])
        ))

    df_preview = pd.read_sql_query("SELECT * FROM sessions LIMIT 10;", conn)
    print(df_preview)

    # 6. Close the database connection
    conn.close()

def getBilling():
    billing = pd.read_csv("billing.csv")
    conn = sqlite3.connect('mydatabase.db')

    # 2. Create a cursor object to execute SQL commands
    cursor = conn.cursor()

    # 3. Select relevant columns
    billing = billing[["user_id", "month", "plan_tier", "active_seats", "mrr", "discount_applied", "invoices_overdue", "support_ticket_count"]]

    create_table_sql = """
    DROP TABLE IF EXISTS billing;
    CREATE TABLE IF NOT EXISTS billing (
        user_id TEXT,
        month TEXT,
        plan_tier TEXT,
        active_seats INTEGER,
        mrr REAL,
        discount_applied REAL,
        invoices_overdue INTEGER,
        support_ticket_count INTEGER
    );
    """

    # 4. Execute the SQL statement
    cursor.executescript(create_table_sql)

    # 5. Commit the changes to the database
    conn.commit()

    insert_sql = """
    INSERT INTO billing (
        user_id, month, plan_tier, active_seats, mrr, discount_applied, invoices_overdue, support_ticket_count
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """

    for index, row in billing.iterrows():
        cursor.execute(insert_sql, (
            str(row['user_id']),
            str(row['month']),
            str(row['plan_tier']),
            int(row['active_seats']),
            float(row['mrr']),
            float(row['discount_applied']),
            int(row['invoices_overdue']),
            int(row['support_ticket_count'])
        ))

    df_preview = pd.read_sql_query("SELECT * FROM billing LIMIT 10;", conn)
    print(df_preview)

    # 6. Close the database connection
    conn.close()



