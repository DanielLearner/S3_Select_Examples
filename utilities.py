import boto3

def get_aws_credentials():
    credentials = {}
    with open('aws_credentials.txt', 'r') as f:
        key = f.readline().strip('\n').split(":")
        credentials[key[0]] = key[1]
        secret = f.readline().strip('\n').split(":")
        credentials[secret[0]] = secret[1]
    return credentials

def get_s3_session_client():
    credentials = get_aws_credentials()
    session = boto3.session.Session(
        aws_access_key_id=credentials["key"],
        aws_secret_access_key=credentials["secret"])
    return session.client('s3')


