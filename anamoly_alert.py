import psycopg2
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from datetime import datetime, timedelta
import numpy as np
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv(dotenv_path="/home/sameen/bot_alerts/credentials.env")

# Connect to Redshift
def connect_to_redshift():
    try:
        print(f"{datetime.now()}: Attempting to connect to Redshift.")
        conn = psycopg2.connect(
            dbname=os.getenv("REDSHIFT_DB"),
            user=os.getenv("REDSHIFT_USER"),
            password=os.getenv("REDSHIFT_PASSWORD"),
            host=os.getenv("REDSHIFT_HOST"),
            port=os.getenv("REDSHIFT_PORT")
        )
        print(f"{datetime.now()}: Successfully connected to Redshift.")
        return conn
    except psycopg2.Error as e:
        print(f"{datetime.now()}: Error: Could not make connection to the Redshift database")
        print(e)
        return None

# Query Redshift for number of orders by hour by day-of-week
def query_orders(conn):
    try:
        print(f"{datetime.now()}: Executing query to fetch order data.")
        cur = conn.cursor()
        cur.execute("""
            SELECT 
                extract(dow from created_at_oli) AS day_of_week,
                extract(hour from created_at_oli) AS hour_of_day,
                count(*) AS order_count
            FROM sales.order_lines_prod
            GROUP BY day_of_week, hour_of_day
            ORDER BY day_of_week, hour_of_day;
        """)
        rows = cur.fetchall()
        print(f"{datetime.now()}: Query executed successfully. Number of rows fetched: {len(rows)}")
        cur.close()
        return rows
    except psycopg2.Error as e:
        print(f"{datetime.now()}: Error: Unable to fetch data from Redshift")
        print(e)
        return None

# Detect anomalies in order counts using IQR method
def detect_anomalies(order_data):
    print(f"{datetime.now()}: Detecting anomalies in order data.")
    # Extract order counts
    order_counts = [count for _, _, count in order_data]
    
    # Calculate percentiles
    q1 = np.percentile(order_counts, 25)
    q3 = np.percentile(order_counts, 75)
    
    # Calculate IQR
    iqr = q3 - q1
    
    # Define threshold for anomaly detection (e.g., 1.5 times IQR)
    threshold = 1.5
    
    # Identify anomalies
    anomalies = [(day, hour, count) for day, hour, count in order_data if count < q1 - threshold * iqr or count > q3 + threshold * iqr]

    print(f"{datetime.now()}: Number of anomalies detected: {len(anomalies)}")
    return anomalies

# Send Slack notification
def send_slack_notification(anomalies):
    if not anomalies:
        print(f"{datetime.now()}: No anomalies to send.")
        return

    try:
        print(f"{datetime.now()}: Sending Slack notification.")
        client = WebClient(token=os.getenv("SLACK_TOKEN"))
        message = "Anomalies detected in order counts:\n"
        for anomaly in anomalies:
            message += f"Day: {anomaly[0]}, Hour: {anomaly[1]}, Order Count: {anomaly[2]}\n"
        response = client.chat_postMessage(channel=os.getenv("SLACK_CHANNEL"), text=message)
        assert response["message"]["text"] == message
        print(f"{datetime.now()}: Slack notification sent successfully.")
    except SlackApiError as e:
        print(f"{datetime.now()}: Error sending Slack notification: {e.response['error']}")

def main():
    print(f"{datetime.now()}: Script started.")
    conn = connect_to_redshift()
    if conn:
        order_data = query_orders(conn)
        if order_data:
            anomalies = detect_anomalies(order_data)
            if anomalies:
                send_slack_notification(anomalies)
            else:
                print(f"{datetime.now()}: No anomalies detected.")
        else:
            print(f"{datetime.now()}: No order data found.")
        conn.close()
        print(f"{datetime.now()}: Redshift connection closed.")
    else:
        print(f"{datetime.now()}: Connection to Redshift failed.")
    print(f"{datetime.now()}: Script finished.")

if __name__ == "__main__":
    main()
