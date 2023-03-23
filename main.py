import os
import requests
import json
from cryptography.fernet import Fernet # 대칭암호화
import uuid
import base64
from datetime import datetime
import time
import gzip
import boto3
from apscheduler.schedulers.blocking import BlockingScheduler

# url을 받아 json 형태로 변환하는 함수
def request_data(url):
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        return data

    else:
        print('Error:', response.status_code)

# # data 복호화
# def decrypt_data(key, data): 
#     _fernet = Fernet(key) # Fernet 계체 생성
#     return _fernet.decrypt(data)

# # 문자열을 json으로 변환하는 함수
# def str_to_json(str):

#     return json.loads(str.replace("'", '"'))

# b64uuid를 uuid로 변환하는 함수
def encode_b64uuid_64(original_uuid):
    # UUID(Universally Unique Identifier)를 바이트 객체로 변환
    uuid_bytes = bytes.fromhex(original_uuid.replace('-', ''))
    # 44자로 Convert
    base64_uuid = base64.urlsafe_b64encode(uuid_bytes).rstrip(b'=').decode('ascii')
    return base64_uuid

# method를 int로 변환하는 함수
def convert_method_to_int(method):
    if method == 'POST':
        return 1
    elif method == 'GET':
        return 2
    elif method == 'PUT':
        return 3
    elif method == 'DELETE':
        return 4
    else:
        return 0

# # 타임스탬프를 datetime으로 변환하는 함수
# def timestamp_to_datetime(timestamp):
    
#     return datetime.fromtimestamp(timestamp)

# # datetime을 타임스탬프로 변환하는 함수
# def datetime_to_timestamp(datetime):

#     return time.mktime(datetime.timetuple())

# # 문자열을 datetime으로 변환하는 함수
# def string_to_datetime(string):

#     return datetime.strptime(string, '%Y-%m-%dT%H:%M:%S.%fZ')

# 문자열을 타임스탬프로 변환하는 함수
def string_to_timestamp(string):
    # 문자열을 datetime으로 변환
    _datetime = datetime.strptime(string, '%Y-%m-%dT%H:%M:%S.%fZ')
    # datetime을 타임스탬프로 변환
    _timestamp = time.mktime(_datetime.timetuple())
    return _timestamp

# # 데이터를 파일로 저장하는 함수
# def dump_data(data):

#     return json.dumps(data)

# # 데이터를 압축하는 함수
# def compress_data(str_data):

#     return gzip.compress(str_data.encode())

# #  dict 데이터를 압축하는 함수
# def compress_dict(dict_data):

#     return compress_data(dump_data(dict_data))
#     return gzip.compress(dump_data(dict_data).encode())

# 하나의 데이터를 받아서 변환을 수행하고 결과를 반환한다.
def convert_single_data(data):
    # 1. 복호화 수행
    key = b't-jdqnDewRx9kWithdsTMS21eLrri70TpkMq2A59jX8='
    # Fernet 계체 생성
    _fernet = Fernet(key) 
    # data 복호화
    decrypt_str = _fernet.decrypt(data['data']).decode('utf-8')
    # 2. 복호화된 데이터를 json(dict)으로 변환한다.
    _json = json.loads(decrypt_str.replace("'", '"'))
    # 3. uuid64 -> 문자열 길이 축소
    _json['user_id'] = encode_b64uuid_64(_json['user_id'])

    # 4. method, int 형으로 변경
    _json['method'] = convert_method_to_int(_json['method'])

    # 5. inDate, timestamp로 변환
    _json['inDate'] = string_to_timestamp(_json['inDate'])

    return _json

# 개인 데이터를 가져오는 함수
def get_private_data():
    with open('data.json', 'r') as f:
        data = json.load(f)
    
    return data['aws_access_key_id'], data['aws_secret_access_key'], data['aws_s3_bucket_name']

# 데이터를 AWS S3에 전송하는 함수
def send_to_aws_s3_path(data, file_path):
    aws_access_key_id, aws_secret_access_key, aws_s3_bucket_name = get_private_data()
    
    # AWS S3에 접근하기 위한 클라 생성
    s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    # AWS S3에 파일 저장
    s3.Object(aws_s3_bucket_name, file_path).put(Body=data)


# -----------------------------------------------------------------------
# -----------------------------------------------------------------------
# 스케줄링
def schedule_job():
    print(f"start schedule job: {datetime.now()}")

    # requests를 사용하여 데이터를 가져온다.
    url = "http://ec2-3-37-12-122.ap-northeast-2.compute.amazonaws.com/api/data/log"
    data = request_data(url)

    _data = {}

    for d in data:
        # 각 데이터에서 개별적으로 변환이 필요한 부분에 변환을 수행한다.
        _json = convert_single_data(d)
        # breakpoint()
        # 타임스탬프를 datetime으로 변환
        _datetime = datetime.fromtimestamp(d['ArrivalTimeStamp'])
        # 연, 월, 일, 시를 출력, 데이터 저장시 사용
        times = [_datetime.year, _datetime.month, _datetime.day, _datetime.hour, _datetime.minute, _datetime.second, _datetime.microsecond // 1000]
        # 키로 사용
        path = f"data/{times[0]}/{times[1]}/{times[2]}/{times[3]}/{times[4]}/"
        
        if path in _data:
            _data[path].append(_json)
        else:
            _data[path] = [_json]

    # print(_data)
    for i in _data:
        # i : 파일 패스
        # _data[i] : 파일 패스에 해당하는 데이터 리스트
        # 데이터를 압축한다.
        json_data = json.dumps(_data[i]) 
        _compress = gzip.compress(json.dumps(_data[i]).encode())
        # breakpoint()
        # Compression ratio: 83.06%
        filepath = i+'log.txt'
        accessParams = get_private_data()
        send_to_aws_s3_path(_compress, filepath, accessParams)

    print('finish schedule job')

# -----------------------------------------------------------------------
# -----------------------------------------------------------------------
# main

if __name__ == "__main__":
    scheduler = BlockingScheduler()
    
    scheduler.add_job(schedule_job, 'interval', seconds=100)

    scheduler.start()

# schedule_job() 

