import boto3
import os
from contextlib import closing
from boto3.dynamodb.conditions import Key, Attr

def lambda_handler(event, context):

    postID = event['Records'][0]['Sns']['Message']
    print(f'Text to Speech Function. Post ID in DynamoDB: {postID}')

    # Retrieving information about the post from DynamoDB table
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(os.environ.get('DB_TABLE_NAME'))
    postItem = table.query(
        KeyConditionExpression=Key('id').eq(postID)
    )

    text = postItem['Items'][0]['text']
    voice = postItem['Items'][0]['voice']

    rest = text

    # Dividing text into blocks of approximately 1000 chars
    # because Polly is only capable of transforming text with 1500 chars
    textBlocks = []
    while len(rest) > 1100:
        begin = 0
        end = rest.find('.', 1000)
        if end == -1:
            end = rest.find(' ', 1000)

        textBlock = rest[begin:end]
        rest = rest[end:]
        textBlocks.append(textBlock)
    textBlocks.append(rest)

    # Process text to speech using polly
    polly = boto3.client('polly')
    for textBlock in textBlocks:
        response = polly.synthesize_speech(
            OutputFormat='mp3',
            Text=textBlock,
            VoiceId=voice
        )
        if 'AudioStream' in response:
            with closing(response['AudioStream']) as stream:
                output = os.path.join('/tmp/', postID)
                with open(output, 'ab') as f:
                    f.write(stream.read())

    # upload mp3 file to S3
    s3 = boto3.client('s3')
    s3.upload_file(f'/tmp/{postID}', os.environ.get('BUCKET_NAME'), f'{postID}.mp3')
    s3.put_object_acl(ACL='public-read', Bucket=os.environ.get('BUCKET_NAME'), Key=f'{postID}.mp3')

    location = s3.get_bucket_location(Bucket=os.environ.get('BUCKET_NAME'))
    region = location['LocationConstraint']

    url_beginning = 'https://s3.amazonaws.com/'
    if region is not None:
        url_beginning = f'https://s3-{region}.amazonaws.com/'

    url = f'{url_beginning}{os.environ.get("BUCKET_NAME")}/{postID}.mp3'

    # updating DynamoDB

    response = table.update_item(
        Key={'id':postID},
        UpdateExpression="SET #statusAtt = :statusValue, #urlAtt = :urlValue",                   
        ExpressionAttributeValues={':statusValue': 'UPDATED', ':urlValue': url},
        ExpressionAttributeNames={'#statusAtt': 'status', '#urlAtt': 'url'}
    )

    return

    