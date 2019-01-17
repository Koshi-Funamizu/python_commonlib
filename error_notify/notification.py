import os
import sys
import boto3
import traceback

sns = boto3.resource('sns')


def notification(func):
    """
    エラー通知デコレータ
    デコレートした関数にエラーがあったときメールを送信します。
    """
    
    def wrapper(*args, **kwds):
        try:
            rtn = func(*args, **kwds)
            return rtn
        except Exception as ex:
            error_notify(str(ex))
            # raise ex
    return wrapper


def error_notify(err):
    # aws sns topic arn
    topic_arn = os.environ['TOPIC_ARN']
    print(f'error_notify_sns_topic_arn : {topic_arn}')

    # メールの内容 
    subject = 'Error occurred.'
    message = 'An error has occurred. Please check out contents below.'
    cause = f'-- cause of error\n\n{err} : {err} {str(type(err))}'
    
    # Tracebackの表示
    trace = traceback.format_exc()
    print(trace)

    # mail 送信 with aws sns
    try:
        sns.Topic(topic_arn).publish(
            Subject=subject,
            Message=message + '\n\n' + cause + '\n\n' + trace
        )
        print('mail sent successfully')
    except Exception as ex:
        print(f'topic_arn: {topic_arn}', f'SNS Publish error occurred. {type(ex)}')
        raise ex

