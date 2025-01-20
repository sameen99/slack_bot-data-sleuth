import psycopg2
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from datetime import datetime, timedelta
import pymysql
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv(dotenv_path="/home/sameen/bot_alerts/credentials.env")

# Function to query sum of sales from RDS
def get_rds_sales_sum(date):
    try:
        print(f"{datetime.now()}: Connecting to RDS database.")
        # Connect to RDS database
        rds_conn = pymysql.connect(
            database=os.getenv("RDS_DB"),
            user=os.getenv("RDS_USER"),
            password=os.getenv("RDS_PASSWORD"),
            host=os.getenv("RDS_HOST"),
            port=int(os.getenv("RDS_PORT"))
        )
        cursor = rds_conn.cursor()
        
        # Query sum of sales for the given date
        query = """SELECT id, avg(total) as total_sales
                   from medusadist_db.orders
                   WHERE DATE(created_at) = CURDATE() - INTERVAL 1 DAY
                   group by id
                   order by id desc;"""
        cursor.execute(query)
        rds_sum = cursor.fetchone()[0] or 0

        cursor.close()
        rds_conn.close()
        print(f"{datetime.now()}: RDS sales sum for {date}: {rds_sum}")
        
        return rds_sum
    except Exception as e:
        print(f"{datetime.now()}: Error querying RDS: {str(e)}")
        return 0

# Function to query sum of sales from Redshift
def get_redshift_sales_sum(date):
    try:
        print(f"{datetime.now()}: Connecting to Redshift database.")
        # Connect to Redshift database
        redshift_conn = psycopg2.connect(
            dbname=os.getenv("REDSHIFT_DB"),
            user=os.getenv("REDSHIFT_USER"),
            password=os.getenv("REDSHIFT_PASSWORD"),
            host=os.getenv("REDSHIFT_HOST"),
            port=int(os.getenv("REDSHIFT_PORT"))
        )
        cursor = redshift_conn.cursor()

        # Query sum of sales for the given date
        query = """SELECT order_id, AVG(order_total) AS total_sales
                   FROM sales.order_lines_prod
                   WHERE DATE_TRUNC('day', created_at_oli) = DATE_TRUNC('day', CURRENT_DATE - INTERVAL '1 day')
                   GROUP BY order_id
                   order by order_id desc"""
        cursor.execute(query)
        redshift_result = cursor.fetchone()

        # Check if result is not None
        if redshift_result is not None:
            redshift_sum = redshift_result[0]  # Access the first column of the result
        else:
            redshift_sum = 0

        cursor.close()
        redshift_conn.close()
        print(f"{datetime.now()}: Redshift sales sum for {date}: {redshift_sum}")
        
        return redshift_sum
    except Exception as e:
        print(f"{datetime.now()}: Error querying Redshift: {str(e)}")
        return 0

# Function to post message to Slack
def post_to_slack(message):
    try:
        print(f"{datetime.now()}: Sending message to Slack.")
        slack_token = os.getenv("SLACK_TOKEN")
        client = WebClient(token=slack_token)
        channel_id = os.getenv("SLACK_CHANNEL")
        client.chat_postMessage(channel=channel_id, text=message)
        print(f"{datetime.now()}: Slack notification sent successfully.")
    except SlackApiError as e:
        print(f"{datetime.now()}: Error sending Slack notification: {e.response['error']}")

def main():
    try:
        print(f"{datetime.now()}: Script started.")
        
        # Calculate date for previous day
        previous_date = datetime.now() - timedelta(days=1)
        previous_date_str = previous_date.strftime('%Y-%m-%d')
        
        # Get sums of sales from RDS and Redshift
        rds_sales_sum = int(get_rds_sales_sum(previous_date_str))
        redshift_sales_sum = int(get_redshift_sales_sum(previous_date_str))
        
        # Report discrepancy
        discrepancy = rds_sales_sum - redshift_sales_sum
        message = f"Discrepancy between RDS and Redshift sales for {previous_date_str}: {discrepancy}"
        post_to_slack(message)
        
        print(f"{datetime.now()}: Script finished.")
    except Exception as e:
        print(f"{datetime.now()}: Error in main function: {str(e)}")

if __name__ == "__main__":
    main()
