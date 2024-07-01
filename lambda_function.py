import json
import boto3
import urllib.parse
import time

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    
    # Get the bucket and object key from the event
    try:
        source_bucket = event['Records'][0]['s3']['bucket']['name']
        source_key = event['Records'][0]['s3']['object']['key']
        print(f"Original Source Key: {source_key}")
        source_key = urllib.parse.unquote_plus(source_key)
        print(f"Decoded Source Key: {source_key}")
        print(f"Source Bucket: {source_bucket}")
    except Exception as e:
        print(f"Error parsing event: {e}")
        raise e

    # Define the backup bucket
    backup_bucket = 'mydestinationbucket654'
    
    # Adding a short delay to ensure the object is fully available
    time.sleep(2)
    
    # Verify the object exists in the source bucket
    try:
        s3_client.head_object(Bucket=source_bucket, Key=source_key)
        print(f"Object {source_key} exists in bucket {source_bucket}")
    except s3_client.exceptions.NoSuchKey as e:
        print(f"Object {source_key} does not exist in bucket {source_bucket}")
        return {
            'statusCode': 404,
            'body': json.dumps(f"Object {source_key} does not exist in bucket {source_bucket}")
        }
    except Exception as e:
        print(f"Error checking object: {e}")
        raise e
    
    # Copy the object
    copy_source = {'Bucket': source_bucket, 'Key': source_key}
    try:
        s3_client.copy_object(CopySource=copy_source, Bucket=backup_bucket, Key=source_key)
        print(f"Successfully copied {source_key} from {source_bucket} to {backup_bucket}")
    except Exception as e:
        print(f"Error copying object: {e}")
        raise e
    
    return {
        'statusCode': 200,
        'body': json.dumps('Backup completed successfully!')
    }
