import json
import time
import logging
import boto3
from datetime import datetime, timedelta, timezone

# データ確認用のバケット
BUCKET = "hirai-yamada"
# 存在を確認するファイル名
FILE_NAME = "*.json"

# ファイルが存在していない場合の待ち時間
WAITING_TIME = 250

# Lambdaのログ出力
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

def lambda_handler(event, _context):
    print(event, _context)
    LOGGER.info(event)

    s3_client = boto3.client("s3")
    lambda_client = boto3.client("lambda")

    # 今日の日付を取得する
    str_dt = get_date()
    # 前日の日付を取得する
    one_days_before = add_days(str_dt, -1)

    # 指定ファイルが存在しているかどうかのステータス取得
    status = check_data(s3_client, one_days_before)
    execute(event, WAITING_TIME, lambda_client, status)

def get_date():
    jst = timezone(timedelta(hours=+9), "JST")
    jst_now = datetime.now(jst)
    dt = datetime.strftime(jst_now, "%Y-%m-%d")

    return dt

def check_data(s3_client, date):

    prefix = date
    response = s3_client.list_objects(
        Bucket = BUCKET,
        Prefix = prefix
    )

    assumed_keys = [f'{date}/{FILE_NAME}']
    try:
        keys = [content['Key'] for content in response['Contents']]
        status = set(assumed_keys).issubset(keys)
    except KeyError:
        status = False

    return status

def add_days(str_dt: str, days: int) -> str:
    datetime_dt = str_to_datetime(str_dt)
    n_days_after = datetime_dt + timedelta(days=days)
    str_n_days_after = datetime_to_str(n_days_after)

    return str_n_days_after

def datetime_to_str(date: datetime) -> str:
    year = str(date.year)
    month = str("{0:02d}".format(date.month))
    day = str("{0:02d}".format(date.day))
    str_date = '{0}-{1}-{2}'.format(year, month, day)

    return str_date

def str_to_datetime(str_date: str) -> datetime:

    return datetime.strptime(str_date, '%Y-%m-%d')

## S3バケット内に対象のファイルがある場合とない場合
def execute(event, WAITING_TIME, lambda_client, status):
    print(event, WAITING_TIME, lambda_client, status)
    if status:  # 対象のバケットにファイルが存在しているとき
        sns_notification_success()

    else:  # 対象のバケットにファイルが存在していないとき
        time.sleep(WAITING_TIME)
        sns_notification_error()

## SNSへ通知
sns = boto3.client('sns')

def sns_notification_success():
    topic = 'arn:aws:sns:ap-northeast-1:040678122487:hirai-yamada-notification'
    subject = '検索結果'
    message = '対象ファイルがありました。'
    region = 'ap-northeast-1'
    response = sns.publish(
            TopicArn=topic,
            Message=message,
            Subject=subject,
            MessageStructure='raw'
        )
    return 'Success'

def sns_notification_error():
    topic = 'arn:aws:sns:ap-northeast-1:040678122487:hirai-yamada-notification'
    subject = '検索結果'
    message = '対象ファイルがありませんでした。'
    region = 'ap-northeast-1'
    response = sns.publish(
            TopicArn=topic,
            Message=message,
            Subject=subject,
            MessageStructure='raw'
        )
    return 'Error'
