import boto3
import os
import uuid

def lambda_handler(event, context):
    
    recordID = str(uuid.uuid1())
    voice = event['voice']
    text = event['text']

    print(f'Generating new DynamoDB record, with ID {recordID}')
    print(f'Input Text: {text}')
    print(f'Selected Voice: {voice}')

    # create new record in DynamoDB Record
    dynamoDB = boto3.resource('dynamodb')
    table = dynamoDB.Table(os.environ.get('DB_TABLE_NAME'))
    table.put_item(
        Item={
            'id' : recordID,
            'text' : text,
            'voice' : voice,
            'status' : 'PROCESSING'
        }
    )

    # sending notifications to new posts through SNS
    sns = boto3.client('sns')
    sns.publish(
        TopicArn = os.environ.get('SNS_TOPIC'),
        Message = recordID
    )

    return recordID


