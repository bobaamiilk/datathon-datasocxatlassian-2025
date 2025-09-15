import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA

def get_downgraded_users():
    conn = sqlite3.connect('mydatabase.db')
    query = "SELECT * FROM users WHERE downgraded = 1"
    downgraded_users = pd.read_sql_query(query, conn)
    conn.close()
    print("Number of downgraded users:", len(df))
    downgraded_users.to_csv("downgraded_users.txt", sep=',', index=False)


def get_is_enterprise_users():
    conn = sqlite3.connect('mydatabase.db')
    query = "SELECT * FROM users WHERE is_enterprise = True"
    enterprise_users = pd.read_sql_query(query, conn)
    conn.close()
    print("Number of enterprise users:", len(enterprise_users))
    enterprise_users.to_csv("enterprise_users.txt", sep=',', index=False)

def plot_churn_rate():
    conn = sqlite3.connect("mydatabase.db")
    df = pd.read_sql_query("SELECT churned_30d, churned_90d FROM users", conn)
    conn.close()
    
    churn_30d = df["churned_30d"].mean()
    churn_90d = df["churned_90d"].mean()
    
    sns.barplot(x=["Churn 30d", "Churn 90d"], y=[churn_30d, churn_90d])
    plt.title("Average Churn Rate")
    plt.ylabel("Churn Rate")
    plt.ylim(0, 1)
    plt.show()

def plot_mrr_by_company_size():
    conn = sqlite3.connect("mydatabase.db")
    df_users = pd.read_sql_query("SELECT user_id, company_size FROM users", conn)
    df_billing = pd.read_sql_query("SELECT user_id, mrr FROM billing", conn)
    conn.close()

    df = pd.merge(df_users, df_billing, on="user_id")
    df_grouped = df.groupby("company_size")["mrr"].mean().sort_values(ascending=False)

    plt.figure(figsize=(10, 6))
    sns.barplot(x=df_grouped.index, y=df_grouped.values)
    plt.title("Average MRR by Company Size")
    plt.xlabel("Company Size")
    plt.ylabel("Average MRR")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def plot_top_features():
    conn = sqlite3.connect("mydatabase.db")
    df = pd.read_sql_query("SELECT feature_name FROM events", conn)
    conn.close()

    top_features = df["feature_name"].value_counts().nlargest(10)

    plt.figure(figsize=(10, 6))
    sns.barplot(x=top_features.values, y=top_features.index, palette='viridis')
    plt.title("Top 10 Most Used Features")
    plt.xlabel("Usage Count")
    plt.ylabel("Feature")
    plt.show()

def detect_trends():
    conn = sqlite3.connect("mydatabase.db")
    users = pd.read_sql_query("SELECT * FROM users", conn)
    conn.close()

    # Keep only numeric columns
    numeric_users = users.select_dtypes(include='number')

    # Plot heatmap
    plt.figure(figsize=(10, 6))
    sns.heatmap(numeric_users.corr(), annot=True, cmap='coolwarm')
    plt.title("Correlation Matrix: Users Table")
    plt.show()

def similar_behaviour_cluster():
    conn = sqlite3.connect("mydatabase.db")
    users = pd.read_sql_query("SELECT * FROM users", conn)
    conn.close()
    features = ["is_enterprise", "churned_30d", "churned_90d", "expansion_event", "downgraded"]
    data = users[features]

    # Normalize
    scaler = StandardScaler()
    data_scaled = scaler.fit_transform(data)

    pca = PCA(n_components=2)
    data_pca = pca.fit_transform(data_scaled)

    # Fit KMeans on reduced data
    kmeans = KMeans(n_clusters=3, random_state=42)
    users["cluster"] = kmeans.fit_predict(data_pca)

    # Visualize clusters
    sns.scatterplot(x=data_pca[:, 0], y=data_pca[:, 1], hue=users["cluster"], palette="Set1")
    plt.title("User Clusters (PCA Reduced)")
    plt.show()


df = pd.read_csv('users.csv')
df['plan_tier'].value_counts().plot(kind='bar')
plt.title('User Count by Plan Tier')
plt.xlabel('Plan Tier')
plt.ylabel('Number of Users')
plt.show()