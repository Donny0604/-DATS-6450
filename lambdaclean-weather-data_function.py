import json
import boto3
import csv
from datetime import datetime

def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    bucket_name = 'myweatherdata-washington-dc'  

    # List all JSON files in the specified bucket
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix='')

    transformed_data = []

    # Process each JSON file
    for item in response.get('Contents', []):
        key = item['Key']
        if key.endswith('.json'):  # Make sure it's a JSON file
            obj = s3_client.get_object(Bucket=bucket_name, Key=key)
            content = obj['Body'].read().decode('utf-8')
            data = json.loads(content)

            for record in data['data']:
                transformed_data.append({
                    'dt': datetime.fromtimestamp(record['dt']).strftime('%Y-%m-%d %H:%M:%S'),
                    'temp': (record['temp'] - 273.15) * 9/5 + 32,  # Convert Kelvin to Fahrenheit
                    'pressure': record.get('pressure', 'N/A'),
                    'humidity': record.get('humidity', 'N/A'),
                    'dew_point': (record.get('dew_point', 273.15) - 273.15) * 9/5 + 32,  # Handle missing dew_point
                    'visibility': record.get('visibility', 'N/A'),  # Provide default if missing
                    'wind_speed': record.get('wind_speed', 'N/A'),
                    'weather': record['weather'][0]['main'] if 'weather' in record and record['weather'] else 'Not Available',
                    'rain_1h': record.get('rain', {}).get('1h', 0)  # Handle missing rain data
                })

    # Generate a single CSV file
    csv_file = '/tmp/transformed_data.csv'
    if transformed_data:  # Check if the list is not empty
        with open(csv_file, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=transformed_data[0].keys())
            writer.writeheader()
            writer.writerows(transformed_data)

        # Upload the CSV file to S3
        csv_key = 'csv/transformed_data.csv'  # Single CSV file
        s3_client.upload_file(csv_file, bucket_name, csv_key)

        return {
            'statusCode': 200,
            'body': json.dumps('Successfully processed and uploaded the CSV file.')
        }
    else:
        return {
            'statusCode': 500,
            'body': json.dumps('No data found or no valid JSON files processed.')
        }
