import boto3
import time

# AWS Configuration
AMI_ID = "ami-0ffa75cb001587739"
INSTANCE_TYPE = "t2.micro"
TOTAL_INSTANCES = 15  # Fixed number of app-tier instances

# AWS Resource Names
REQ_QUEUE = "1229564013-req-queue"
IAM_ROLE = "App-Tier-Access-IAM-Role"
SECURITY_GROUP = "launch-wizard-5"
KEY_NAME = "app-tier-instance"

# AWS Clients
ec2 = boto3.client("ec2", region_name="us-east-1")
sqs = boto3.client("sqs", region_name="us-east-1")

# Get queue URL
queue_url = "https://sqs.us-east-1.amazonaws.com/476114119085/1229564013-req-queue"

def get_queue_length():
    """Check the number of messages in the SQS request queue."""
    response = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["ApproximateNumberOfMessages"])
    return int(response["Attributes"]["ApproximateNumberOfMessages"])

def get_instances(state):
    """Get all App-Tier instances in a given state (running or stopped)."""
    response = ec2.describe_instances(
        Filters=[
            {"Name": "instance-state-name", "Values": [state]},
            {"Name": "image-id", "Values": [AMI_ID]}  # Only App-Tier instances
        ]
    )

    instances = [instance["InstanceId"] for res in response["Reservations"] for instance in res["Instances"]]
    return instances

def launch_initial_instances():
    """Launch exactly 15 app-tier instances with backend.py auto-start on boot, then stop them."""
    existing_instances = get_instances("running") + get_instances("stopped")

    if len(existing_instances) >= TOTAL_INSTANCES:
        print(f"{TOTAL_INSTANCES} instances already exist. Skipping creation.")
        return

    new_instances_needed = TOTAL_INSTANCES - len(existing_instances)
    print(f"Launching {new_instances_needed} new App-Tier instances...")

    user_data_script = """#!/bin/bash
            source /home/ubuntu/myenv/bin/activate
            cd /home/ubuntu
            python3 backend.py
        """

    instances = ec2.run_instances(
        ImageId=AMI_ID,
        InstanceType=INSTANCE_TYPE,
        MinCount=new_instances_needed,
        MaxCount=new_instances_needed,
        KeyName=KEY_NAME,
        SecurityGroups=[SECURITY_GROUP],
        IamInstanceProfile={'Name': IAM_ROLE},
        UserData=user_data_script
    )

    instance_ids = [instance["InstanceId"] for instance in instances["Instances"]]  #  Define `instance_ids`

    # Assign unique names to new instances
    for i, instance_id in enumerate(instance_ids):
        instance_name = f"app-tier-instance-{i+1}"
        ec2.create_tags(Resources=[instance_id], Tags=[{"Key": "Name", "Value": instance_name}])
        print(f"Assigned name {instance_name} to instance {instance_id}")

    #  Wait until all instances reach "running" state before stopping them
    print("Waiting for instances to reach 'running' state before stopping them...")
    for _ in range(10):  # Check up to 10 times (approx 2 minutes)
        running_instances = get_instances("running")
        if all(inst_id in running_instances for inst_id in instance_ids):
            print("All instances are now running. Proceeding to stop them.")
            break
        time.sleep(12)  # Wait 12 seconds before checking again
    else:
        print("Warning: Some instances did not reach running state within timeout. Attempting to stop them anyway.")

    #  Stop all newly created instances
    if instance_ids:
        print("Stopping all newly launched instances...")
        ec2.stop_instances(InstanceIds=instance_ids)
    else:
        print("No new instances were created, skipping stop command.")


def scale_instances():
    """Monitor SQS queue and start stopped instances as needed."""
    launch_initial_instances()  # Ensure 15 instances exist on first run

    while True:
        queue_length = get_queue_length()
        print("Queue Length = ", queue_length)

        running_instances = len(get_instances("running"))
        stopped_instances = get_instances("stopped")
        num_stopped = len(stopped_instances)

        print("Running Instances =", running_instances)
        print("Stopped Instances =", num_stopped)

        if queue_length > 0 and num_stopped > 0:
            # Start up to min(queue_length, num_stopped)
            instances_to_start = min(queue_length, num_stopped)
            print(f"Starting {instances_to_start} stopped instances...")
            ec2.start_instances(InstanceIds=stopped_instances[:instances_to_start])

        time.sleep(10)  # Check every 10 seconds

if __name__ == "__main__":
    scale_instances()
