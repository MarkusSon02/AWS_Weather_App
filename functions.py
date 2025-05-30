from datetime import datetime, timedelta
import boto3
import json


def partition_data(data: dict) -> tuple:
    country = data['location']['country']
    location = data["location"]['name']
    return country, location

def partition_date(date: str) -> tuple:
    dt = datetime.strptime(date, "%Y-%m-%d")
    year = dt.year
    month = f"{dt.month:02d}"
    day = f"{dt.day:02d}"
    return year, month, day

def is_dict(data: dict) -> bool:
    return isinstance(data, dict)

def generate_date_list(start: str, end: str) -> list:
    start = datetime.strptime(start, "%Y-%m-%d")
    end = datetime.strptime(end, "%Y-%m-%d")

    date_list = [ str((end - timedelta(days=i)).strftime("%Y-%m-%d"))
                    for i in range((end - start).days + 1) ]

    return date_list

def upload_partitioned_weather(data: dict, partitions: tuple, bucket: str) -> None:
    """
    Uploads JSON data to S3 using a partitioned path:
    s3://<bucket>/weather_data/country=<country>/location=<location>/year=YYYY/month=MM/weather.json
    """
    
    country, location, year, month, day = partitions

    s3 = boto3.client("s3")

    key = f"data/weather_data/country={country}/location={location}/year={year}/month={month}/{location}_weather_{year}-{month}-{day}.json"
    
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data),
        ContentType="application/json"
    )
    
    print(f"Uploaded to s3://{bucket}/{key}")

