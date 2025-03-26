import boto3
import os
import subprocess
import time
import json
import requests

# AWS Configuration
REQ_QUEUE = "1229564013-req-queue"
RES_QUEUE = "1229564013-res-queue"
IN_BUCKET = "1229564013-in-bucket"
OUT_BUCKET = "1229564013-out-bucket"

# AWS Clients
sqs = boto3.client("sqs", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
ec2 = boto3.client("ec2", region_name="us-east-1")

# Queue URLs
req_queue_url = "https://sqs.us-east-1.amazonaws.com/476114119085/1229564013-req-queue"
res_queue_url = "https://sqs.us-east-1.amazonaws.com/476114119085/1229564013-resp-queue"

# Path to the face recognition model script
MODEL_SCRIPT = "/home/ubuntu/CSE546-SPRING-2025/face_recognition.py"

def download_image_from_s3(image_name):
    """Download image from input S3 bucket."""
    local_path = f"/home/ubuntu/CSE546-SPRING-2025/{image_name}"
    s3.download_file(IN_BUCKET, image_name, local_path)
    return local_path

def upload_result_to_s3(image_name, result):
    """Upload recognition result to output S3 bucket."""
    s3.put_object(Bucket=OUT_BUCKET, Key=image_name, Body=result)

def send_result_to_sqs(image_name, result, correlation_id):
    """Send the recognition result to the response SQS queue."""
    message_body = f"{image_name}:{result}:{correlation_id}"
    sqs.send_message(QueueUrl=res_queue_url, MessageBody=message_body)

def get_instance_id():
    """Get the current EC2 instance ID using IMDSv2."""
    try:
        token_url = "http://169.254.169.254/latest/api/token"
        headers = {"X-aws-ec2-metadata-token-ttl-seconds": "21600"}  # Token valid for 6 hours
        response = requests.put(token_url, headers=headers)
        token = response.text

        instance_id_url = "http://169.254.169.254/latest/meta-data/instance-id"
        headers = {"X-aws-ec2-metadata-token": token}
        response = requests.get(instance_id_url, headers=headers)
        return response.text.strip()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching instance ID: {e}")
        return None

def stop_instance():
    """Stop the current EC2 instance."""
    instance_id = get_instance_id()
    if instance_id:
        print(f"Stopping instance {instance_id}...")
        ec2.stop_instances(InstanceIds=[instance_id])
    else:
        print("Failed to retrieve instance ID.")

def process_request():
    """Process a single image request from SQS."""
    while True:
        # Receive a message from SQS request queue
        response = sqs.receive_message(
            QueueUrl=req_queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=5
        )

        messages = response.get("Messages", [])
        if not messages:
            stop_instance()
            break
            #time.sleep(2)  # Sleep if no messages are found
            #continue

        for message in messages:
            receipt_handle = message["ReceiptHandle"]
            body = json.loads(message["Body"])  # Correct decoding
            image_name = body['filename']
            correlation_id = body['correlation_id']
            print(f"Processing: {image_name}")

            # Download image from S3
            image_path = download_image_from_s3(image_name)

            # Run inference using the model
            try:
                result = subprocess.check_output(
                    ["python3", MODEL_SCRIPT, image_path], text=True
                ).strip()
            except Exception as e:
                print(f"Error in model inference: {e}")
                result = "Unknown"

            # Upload result to output S3 bucket
            upload_result_to_s3(image_name, result)

            # Send result to SQS response queue
            send_result_to_sqs(image_name, result, correlation_id)

            # Delete processed message from SQS request queue
            sqs.delete_message(QueueUrl=req_queue_url, ReceiptHandle=receipt_handle)

            print(f"Processed {image_name}: {result}")

if __name__ == "__main__":
    print("App-Tier instance started...")
    process_request()
