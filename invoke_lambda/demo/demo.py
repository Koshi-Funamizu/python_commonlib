from invoke_lambda import invoke_lambda

lambda_function_name = 'kfunamizu_sample'

event_dict = {
    '1' : '1',
    '2' : '2',
    '3' : '3'
    }

invoke_lambda(lambda_function_name, event_dict)
