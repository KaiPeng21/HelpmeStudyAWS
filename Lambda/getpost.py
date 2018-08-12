import boto3
import os
from boto3.dynamodb.conditions import Key, Attr

def lambda_handler(event, context):

    postID = event['postId']

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(os.environ.get('DB_TABLE_NAME'))

    if postID == '*':
        items = table.scan()
    else:
        items = table.query(
            KeyConditionExpression=Key('id').eq(postID)
        )

    return items['Items']