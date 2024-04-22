import json
import requests
import boto3
from datetime import datetime, timedelta

def lambda_handler(event, context):
    api_key = '89ce297ce53e2fe03efeb5d2ea33e524'
    lat, lon = '38.9072', '-77.0369'  # Coordinates for Washington, D.C.
    dt = int((datetime.now() - timedelta(days=1)).timestamp())  # Yesterday's data

    url = f'https://api.openweathermap.org/data/3.0/onecall/timemachine?lat={lat}&lon={lon}&dt={dt}&appid={api_key}'
    response = requests.get(url)
    data = response.json()

    s3 = boto3.client('s3')
    bucket_name = 'myweatherdata-washington-dc'
    s3.put_object(Bucket=bucket_name, Key=f'weather_data_{dt}.json', Body=json.dumps(data))

    return {
        'statusCode': 200,
        'body': json.dumps('Data fetched and stored successfully')
    }
