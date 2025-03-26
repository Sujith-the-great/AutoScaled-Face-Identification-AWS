from flask import Flask, request
import boto3  ## Reference : https://boto3.amazonaws.com/v1/documentation/api/latest/guide/index.html
import os
import json
import uuid
import time

app = Flask(__name__)

## Defined my bucket and domain names here.
s3b = "1229564013-in-bucket" # ASUID-in-bucket
sdbd = "1229564013-simpleDB" # ASUID-simpleDB
req_queue_url = "https://sqs.us-east-1.amazonaws.com/476114119085/1229564013-req-queue"
res_queue_url = "https://sqs.us-east-1.amazonaws.com/476114119085/1229564013-resp-queue"

## create objects of s3, sqs and sdb using boto3 sdk
s3 = boto3.client('s3', region_name='us-east-1')
sdb = boto3.client('sdb', region_name='us-east-1')
sqs = boto3.client('sqs', region_name='us-east-1')


# For POST requests
@app.route("/", methods=["POST"])
def upload_and_enqueue():
    if "inputFile" not in request.files:
        return "No file part", 400

    fl = request.files["inputFile"]
    fln = fl.filename
    s3.upload_fileobj(fl, s3b, fln)  # Upload to S3

    # Generate a correlation ID
    correlation_id = str(uuid.uuid4())

    # Send message to Request Queue with correlation ID
    message = {"filename": fln, "correlation_id": correlation_id}
    sqs.send_message(QueueUrl=req_queue_url, MessageBody=json.dumps(message))
    print(f"Message {message} sent to queue")
    # Store the correlation ID for later use
    request_correlation_id = correlation_id

    # Poll response queue for result with timeout
    timeout = 600  # seconds
    start_time = time.time()
    result = None
    while time.time() - start_time < timeout:
        print("In while loop")
        response = sqs.receive_message(QueueUrl=res_queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=20)
        break_var = False
        if "Messages" in response:
          for message in response["Messages"]:

            body = message["Body"]
            print("Currently looking at ",body, "for ",fln)
            # Check if the message belongs to this request
            if request_correlation_id in body:
                # Process the message and send the result
                sqs.delete_message(QueueUrl=res_queue_url, ReceiptHandle=message["ReceiptHandle"])
                parts = body.split(":")
                if len(parts) == 3:
                    filename = parts[0]
                    prediction = parts[1]
                    result = f"{filename}:{prediction}"
                    print(result)
                else:
                    print("Error parsing message body.")
                    result = "Error parsing result."
                # Delete message from Response Queue
                break_var = True
                break
          if break_var:
             break
        #time.sleep(1)  # Poll every second

    if result is None:
        return "Result not available within timeout.", 500

    return result, 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, threaded=True)
