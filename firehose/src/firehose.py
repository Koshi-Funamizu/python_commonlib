import boto3, datetime, time


region = 'us-west-2'
accesskey = os.environ['ACCESSKEY']
secretkey = os.environ['SECRET_ACCESSKEY']

def send_firehose(message: str):
  client = boto3.client('firehose', aws_access_key_id=accesskey, aws_secret_access_key=secretkey, region_name=region)

  # Send message to firehose
  response = client.put_record(
    DeliveryStreamName=os.environ['FIREHOSE_NAME'],
      Record={
        'Data': message
          }
        )
  
  return response
