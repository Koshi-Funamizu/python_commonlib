import boto3

dynamodb = boto3.resource('dynamodb')


def put_item(table_name, item):
    try:
        table = dynamodb.Table(table_name)
        table.put_item(Item=item)
    except Exception as ex:
        return None


def put_items(table_name, items):
    try:
        table = dynamodb.Table(table_name)
        with table.batch_writer() as batch:
            for item in items:
                batch.put_item(Item=item)
    except Exception as ex:
        return None


def get_item(table_name, key, value):
    try:
        table = dynamodb.Table(table_name)
        response = table.get_item(
            Key={
                key: value
            }
        )
        if 'Item' in response:
            return response['Item']

        return []
    except Exception as ex:
        return None
