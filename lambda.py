import boto3
import json
import datetime
import os
from botocore.errorfactory import ClientError

print('Loading function')
dynamo = boto3.client('dynamodb')
TABLE = os.environ['TABLE']
BUCKET = os.environ['BUCKET']


def error_response(message):
    return ({
        'statusCode': '200',
        'body': json.dumps({'ok': False, 'error': message}),
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
        },
    })


def success_response(url):
    return ({
        'statusCode': '200',
        'body': json.dumps({'ok': True, 's3_url': url}),
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
        },
    })


def lambda_handler(event, context):
    if event['httpMethod'] != 'GET':
        return error_response('invalid method')

    ExpressionAttributeValues = dict()
    FilterExpression = []
    params = event['queryStringParameters']

    if params is None:
        params = {'to': datetime.datetime.now().strftime('%Y-%m-%d-%H-%M')}
    else:
        if 'from' in params:
            if len(params['from']) == 0:
                del params['from']
            else:
                try:
                    params['from'] = datetime.datetime.strptime(params['from'], '%Y-%m-%d-%H-%M').isoformat()
                except Exception as e:
                    return error_response((e.fmt if hasattr(e, 'fmt') else '') + ','.join(e.args))

                ExpressionAttributeValues[':from'] = {
                    'S': params['from']}
                FilterExpression.append('created_at > :from')

        if 'to' in params:
            if len(params['to']) == 0:
                params['to'] = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M')
            else:
                try:
                    params['to'] = datetime.datetime.strptime(params['to'], '%Y-%m-%d-%H-%M').isoformat()
                except Exception as e:
                    return error_response((e.fmt if hasattr(e, 'fmt') else '') + ','.join(e.args))

        else:
            params['to'] = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M')

    ExpressionAttributeValues[':to'] = {
        'S': params['to']}
    FilterExpression.append('created_at < :to')

    filename = 'twitter/%s_%s.json' % (params['from'] if 'from' in params else 'infinite', params['to'])
    s3 = boto3.client('s3')
    try:
        s3.head_object(Bucket=BUCKET, Key=filename)
        # The json file already been created
        return success_response('%s/%s/%s' % (s3.meta.endpoint_url, BUCKET, filename))
    except ClientError:
        # Not found
        try:
            if FilterExpression:
                tweets = dynamo.scan(
                    TableName=TABLE,
                    ProjectionExpression='c0, c1',
                    FilterExpression=' and '.join(FilterExpression),
                    ExpressionAttributeValues=ExpressionAttributeValues
                )
            else:
                tweets = dynamo.scan(
                    TableName=TABLE,
                    ProjectionExpression='c0, c1'
                )

            geo_data = {
                "type": "FeatureCollection",
                "features": []
            }

            for tweet in tweets['Items']:
                geo_json_feature = {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [tweet['c0']['S'], tweet['c1']['S']]
                    }
                }
                geo_data['features'].append(geo_json_feature)

            s3 = boto3.resource('s3')
            s3.Object(BUCKET, filename).put(Body=json.dumps(geo_data, indent=4))
            s3.ObjectAcl(BUCKET, filename).put(ACL='public-read')

            return success_response('%s/%s/%s' % (s3.meta.client.meta.endpoint_url, BUCKET, filename))
        except Exception as e:
            return error_response((e.fmt if hasattr(e, 'fmt') else '') + ','.join(e.args))


### Do not copy the Python code below this comment
### Use different examples to debug and test the code

print('--------------------GET event test')
get_event = {
    "httpMethod": "GET",
    "queryStringParameters": {
        "from": "2018-04-16-10-10"
    }
}
result = lambda_handler(get_event, None)
print('--------------------RESULT')
print(json.dumps(result, indent=2))
print('--------------------RESULT body')
print(json.dumps(json.loads(result['body']), indent=2))

print('--------------------GET event test')
get_event = {
    "httpMethod": "GET",
    "queryStringParameters": {
        "to": "2018-04-16-15-10"
    }
}
result = lambda_handler(get_event, None)
print('--------------------RESULT')
print(json.dumps(result, indent=2))
print('--------------------RESULT body')
print(json.dumps(json.loads(result['body']), indent=2))

print('--------------------GET event test')
get_event = {
    "httpMethod": "GET",
    "queryStringParameters":None
}
result = lambda_handler(get_event, None)
print('--------------------RESULT')
print(json.dumps(result, indent=2))
print('--------------------RESULT body')
print(json.dumps(json.loads(result['body']), indent=2))
