import boto3
import csv
import json
from datetime import datetime
import uuid
import os
from requests_aws4auth import AWS4Auth
import requests

BUCKET_NAME = os.environ['BUCKET_NAME']
META_DATA_FILE = os.environ['META_DATA_FILE']
ENGINES = os.environ['ENGINES'].split(',')
ID_COL_NAME = os.environ['ID_COL_NAME']
FEATURE_COL_NAMES = os.environ['FEATURE_COL_NAMES'].split(',')
APPSYNC_ENDPOINT = os.environ['APPSYNC_ENDPOINT']

s3_client = boto3.client('s3')
dynamo_db_client = boto3.client('dynamodb')


def get_app_sync_session():
    session = requests.Session()
    credentials = boto3.session.Session().get_credentials()
    session.auth = AWS4Auth(
        credentials.access_key,
        credentials.secret_key,
        boto3.session.Session().region_name,
        'appsync',
        session_token=credentials.token
    )
    return session


def lambda_handler(event, context):
    try:
        # Step 1: Get the last data index from the meta_data.json file.
        meta_data_obj = s3_client.get_object(
            Bucket=BUCKET_NAME, Key=META_DATA_FILE)
        meta_data_content = meta_data_obj['Body'].read().decode('utf-8-sig')

        json_content = json.loads(meta_data_content)
        data_index = json_content['last_data_index']

        # Step 2: For each engine, get the row with index equals to the data index. Then upload data in that row to the AppSync API.
        for engine in ENGINES:
            csv_filename = engine.lower() + '.csv'
            csv_file = s3_client.get_object(
                Bucket=BUCKET_NAME, Key=csv_filename)
            records_list = csv_file['Body'].read().decode(
                'utf-8-sig').splitlines()

            csv_reader = csv.DictReader(records_list, delimiter=',')
            row_count_w_header = sum(
                1 for row in csv.reader(records_list, delimiter=','))
            #print('Row count with header: ', json.dumps(row_count_w_header))

            if data_index < row_count_w_header:
                row = {}
                for i in range(0, data_index):
                    if(i == data_index):
                        continue
                    else:
                        row = next(csv_reader)
                # print(row)

                unit_number = row[ID_COL_NAME]
                date_time_now = datetime.now()
                formatted_date_time = date_time_now.strftime(
                    "%Y-%m-%dT%H:%M:%SZ")

                app_sync_session = get_app_sync_session()
                cm_data = {}

                for feature in FEATURE_COL_NAMES:
                    value = 0
                    if row[feature]:
                        value = row[feature]
                    cm_data[feature] = value

                input_data = {
                    'id': uuid.uuid4().hex,
                    'unitNumber': unit_number,
                    'dateTime': formatted_date_time,
                    'data': json.dumps(cm_data)
                }

                mutation_query = """mutation CreateConditionMonitoringDataRecord($input: CreateConditionMonitoringDataRecordInput!) {
                                    createConditionMonitoringDataRecord(input: $input) {
                                        __typename
                                        id
                                        unitNumber
                                        dateTime
                                        data
                                        createdAt
                                        updatedAt
                                    }
                                }
                                        """
                variables = {'input': input_data}

                app_sync_response = app_sync_session.request(
                    url=APPSYNC_ENDPOINT,
                    method='POST',
                    json={'query': mutation_query, 'variables': variables}
                )
                #print("AppSync response: ", json.dumps(app_sync_response.text))

        # Step 3: Update the data index to the new value and store it in the meta_data.json.
        new_meta_data = {'last_data_index': data_index+1}
        s3_client.put_object(
            Body=(bytes(json.dumps(new_meta_data).encode('UTF-8'))),
            Bucket=BUCKET_NAME,
            Key=META_DATA_FILE
        )

        res = {
            'statusCode': 200,
            'body': json.dumps({'success': True, 'data_index': data_index})
        }
        return res
    except Exception as e:
        res = {
            'statusCode': 500,
            'body': json.dumps(str(e))
        }
        return res
