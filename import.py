import pandas as pd
import sqlite3
import json

def getUserById(user_id, conn):
    query = "SELECT * FROM users WHERE user_id = ?"
    df = pd.read_sql_query(query, conn, params=(user_id,))
    if df.empty:
        return None
    # Return as JSON string
    return df.to_json(orient="records")

def getSessionById(session_id, conn):
    query = "SELECT * FROM sessions WHERE session_id = ?"
    df = pd.read_sql_query(query, conn, params=(session_id,))
    if df.empty:
        return None
    return df.to_json(orient="records")

def getUsers():
    users = pd.read_csv("users.csv", keep_default_na=False)
    # 1. Connect to the database (or create it if it doesn't exist)
    #    This will create a file named 'mydatabase.db' in the current directory.
    conn = sqlite3.connect('mydatabase.db')

    # 2. Create a cursor object to execute SQL commands
    cursor = conn.cursor()

    # 3. Define the SQL CREATE TABLE statement
    #    This example creates a table named 'users' with 'id', 'name', and 'email' columns.

    users = users[["user_id","signup_date","plan_tier","company_size","region","industry","acquisition_channel","is_enterprise","churned_30d","churned_90d","downgraded","expansion_event"]]

    users["session_ids"] = '[]'
    users["billing"] = '[]'

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
        expansion_event INTEGER,
        session_ids TEXT,
        billing TEXT
    );
    """

    # 4. Execute the SQL statement
    cursor.executescript(create_table_sql)

    # 5. Commit the changes to the database
    conn.commit()

    insert_sql = """
    INSERT INTO users (
        user_id, signup_date, plan_tier, company_size, region, industry, acquisition_channel,
        is_enterprise, churned_30d, churned_90d, downgraded, expansion_event, session_ids, billing
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            int(row['expansion_event']),
            str(row['session_ids']),  # This will be '[]' initially
            str(row['billing'])  # This will be '[]' initially
        ))
    
    conn.commit()
    df_preview = pd.read_sql_query("SELECT * FROM users LIMIT 10;", conn)
    print(df_preview)

    # 6. Close the database connection
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
        session_length_sec INTEGER,
        event_ids TEXT
    );
    """

    sessions["event_ids"] = '[]'

    # 4. Execute the SQL statement
    cursor.executescript(create_table_sql)

    # 5. Commit the changes to the database
    conn.commit()

    insert_sql = """
    INSERT INTO sessions (
        session_id, user_id, session_start, session_end, device, os, app_version, country, session_length_sec, event_ids
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            int(row['session_length_sec']),
            str(row['event_ids']) # This will be '[]' initially
        ))

        

    conn.commit()
    df_preview = pd.read_sql_query("SELECT * FROM sessions LIMIT 10;", conn)
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
    
    conn.commit()
    df_preview = pd.read_sql_query("SELECT * FROM events LIMIT 10;", conn)
    print(df_preview)

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
    CREATE TABLE IF NOT EXISTS billing(
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
    conn.commit()
    df_preview = pd.read_sql_query("SELECT * FROM billing LIMIT 10;", conn)
    print(df_preview)

    # 6. Close the database connection
    conn.close()

def add_session_to_user(user_id, new_session_id, conn):
    # Get current session_ids
    cursor = conn.cursor()
    cursor.execute("SELECT session_ids FROM users WHERE user_id = ?", (user_id,))
    row = cursor.fetchone()
    if row:
        session_ids = json.loads(row[0]) if row[0] else []
        if new_session_id not in session_ids:
            session_ids.append(new_session_id)
            cursor.execute(
                "UPDATE users SET session_ids = ? WHERE user_id = ?",
                (json.dumps(session_ids), user_id)
            )

def add_event_to_session(session_id, new_event_id, conn):
    # Get current event_ids
    cursor = conn.cursor()
    cursor.execute("SELECT event_ids FROM sessions WHERE session_id = ?", (session_id,))
    row = cursor.fetchone()
    if row:
        event_ids = json.loads(row[0]) if row[0] else []
        if new_event_id not in event_ids:
            event_ids.append(new_event_id)
            cursor.execute(
                "UPDATE sessions SET event_ids = ? WHERE session_id = ?",
                (json.dumps(event_ids), session_id)
            )

def add_bill_to_user(user_id, new_bill, conn):
    cursor = conn.cursor()
    cursor.execute("SELECT billing FROM users WHERE user_id = ?", (user_id,))
    row = cursor.fetchone()
    if row:
        bills = json.loads(row[0]) if row[0] else []
        bills.append(new_bill)
        cursor.execute(
            "UPDATE users SET billing = ? WHERE user_id = ?",
            (json.dumps(bills), user_id)
        )

def connect_users_sessions():
    conn = sqlite3.connect('mydatabase.db')
    sessions = pd.read_csv("sessions.csv")
    for index, row in sessions.iterrows():
        add_session_to_user(row['user_id'], row['session_id'], conn)

    conn.commit()

    df_preview = pd.read_sql_query("SELECT * FROM users LIMIT 10;", conn)
    print(df_preview)

    conn.close()

def connect_sessions_events():
    conn = sqlite3.connect('mydatabase.db')
    events = pd.read_csv("events.csv")
    for index, row in events.iterrows():
        add_event_to_session(row['session_id'], row['event_id'], conn)

    conn.commit()

    conn.close()

def connect_bills_to_users():
    conn = sqlite3.connect('mydatabase.db')
    billing = pd.read_csv("billing.csv")
    for index, row in billing.iterrows():
        add_bill_to_user(row['user_id'], row.to_dict(), conn)

    conn.commit()

    df_preview = pd.read_sql_query("SELECT * FROM users LIMIT 10;", conn)
    print(df_preview)

    conn.close()
#getUsers()
# Must run getUsers() before getSessions()
getUsers()
getSessions()
getEvents()
getBilling()
connect_sessions_events()
connect_users_sessions()
connect_bills_to_users()
