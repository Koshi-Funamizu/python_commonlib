import boto3
import json

lambda_client = boto3.client('lambda')


# Lambda Invoke関数
def invoke_lambda(lambda_function_name: str, event_dict=None):
    if event_dict == None:
        event_dict = {
            "key1": "value1",
            "key2": "value2",
            "key3": "value3"
            }
    
    response = lambda_client.invoke(
            FunctionName=lambda_function_name,
            InvocationType='RequestResponse',
            LogType='Tail',
            Payload=json.dumps(event_dict)
        )
