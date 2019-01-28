import boto3

def assumerole(assume_role_arn, s2_assume_role_session_id)
	# Assume Role実行
    try:
    	sts_client = boto3.client('sts')
    	sts_resp = sts_client.assume_role(RoleArn=s2_assume_role_arn, RoleSessionName=s2_assume_role_session_id)
    	session = boto3.Session(aws_access_key_id=sts_resp['Credentials']['AccessKeyId'],
    		aws_secret_access_key=sts_resp['Credentials']['SecretAccessKey'],
    		aws_session_token=sts_resp['Credentials']['SessionToken'],
    		region_name=os.environ.get('AWS_DEFAULT_REGION', 'us-west-2'))
    except Exception as ex:
    	raise (f'AssumeRole Error occurred. {str(ex)}')

    	return session
