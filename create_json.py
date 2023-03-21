import json

# json 파일 경로
json_file_path = 'data.json'

# 쓸 데이터 생성
data = {
    'aws_access_key_id': '-',
    'aws_secret_access_key': '-',
    'aws_s3_bucket_name': '-'
}

# JSON 파일에 쓰기
with open(json_file_path, 'w') as f:
    json.dump(data, f, indent=4)