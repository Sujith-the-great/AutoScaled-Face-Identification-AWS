from flask import Flask, request
import boto3  ## Reference : https://boto3.amazonaws.com/v1/documentation/api/latest/guide/index.html
import os

app = Flask(__name__)

## Defined my bucket and domain names here.
s3b = "1229564013-in-bucket" # ASUID-in-bucket
sdbd = "1229564013-simpleDB" # ASUID-simpleDB

## create objects of s3 and sdb using boto3 sdk
s3 = boto3.client('s3', region_name='us-east-1')
sdb = boto3.client('sdb', region_name='us-east-1')


# For POST requests
@app.route("/", methods=["POST"])
def upload_and_lookup():
    if "inputFile" not in request.files: # No image in request
        return "No file part", 400

    fl = request.files["inputFile"]  # Get file from request
    fln = fl.filename # Get filename from File
    b_fln = os.path.splitext(fln)[0] # Remove .jpg

    s3.upload_fileobj(fl,s3b , b_fln) # Upload this file to bucket

    response = sdb.get_attributes(DomainName=sdbd, ItemName=b_fln) # Lookup in SimpleDb the file
    attributes = response.get("Attributes", []) # Get the lookup's object's attributes

    if not attributes: # No attributes means no Info on it in SimpleDB
        return f"{b_fln}:Unknown", 200

    person_name = attributes[0]["Value"] # Get the person's name defined in "Value"
    return f"{b_fln}:{person_name}", 200

## Using FLask Development server instead of apache
## Port : 8000 - asked in project
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
