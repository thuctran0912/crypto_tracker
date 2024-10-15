import os
import json
import boto3
import finnhub
from datetime import datetime

# Initialize S3 client
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # Retrieve the API key from environment variables
    api_key = os.getenv("FINNHUB_API_KEY")

    # Check if the API key is set
    if not api_key:
        return {
            "statusCode": 500,
            "body": "API key not set. Please set the FINNHUB_API_KEY environment variable."
        }

    # Create a Finnhub client
    finnhub_client = finnhub.Client(api_key=api_key)

    # Fetch general news
    news = finnhub_client.general_news('crypto', min_id=0)

    # Convert news data to JSON string
    news_json = json.dumps(news)

    # Specify the S3 bucket and generate the file name with the current date
    s3_bucket_name = "import-finnhub"
    current_date = datetime.now().strftime("%Y%m%d")
    s3_file_name = f"crypto_news__{current_date}.json"

    try:
        # Save the news data to the S3 bucket
        s3_client.put_object(Bucket=s3_bucket_name, Key=s3_file_name, Body=news_json)

        return {
            "statusCode": 200,
            "body": f"News data saved to S3 bucket '{s3_bucket_name}' as '{s3_file_name}'"
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"Error saving to S3: {str(e)}"
        }