from notification import notification


@notification
def test():
    x = 1 / 0 # ZeroDivisionError

test()
