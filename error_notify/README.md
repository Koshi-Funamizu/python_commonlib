# notification.py
aws snsでエラーログをメールで送信するプログラム

# 使用例
```
>>> from notification import notification
>>>
>>> @notification
... def test():
...     x = 1 / 0 # ZeroDivisionError
...
>>> test()
error_notify_sns_topic_arn : arn:aws:sns:us-west-2:168002149898:kfunamizu-etl-pipeline-test
Traceback (most recent call last):
  File "/home/ec2-user/work/notification.py", line 17, in wrapper
    rtn = func(*args, **kwds)
  File "<stdin>", line 3, in test
ZeroDivisionError: division by zero

mail sent successfully
```

# 送られてきたメール
```
Error occurred. (件名)
AWS Notifications [no-reply@sns.amazonaws.com] (送り元)


An error has occurred. Please check out contents below.

-- cause of error

division by zero : division by zero <class 'str'>

Traceback (most recent call last):
  File "/home/ec2-user/work/notification.py", line 17, in wrapper
    rtn = func(*args, **kwds)
  File "<stdin>", line 3, in test
ZeroDivisionError: division by zero

```
