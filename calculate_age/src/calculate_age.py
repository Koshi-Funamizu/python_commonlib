import sys
import re
import datetime as dt

def calculate_age(day_of_birth: str):
    # 区切り文字削除
    day_of_birth = re.sub(r'[^0-9]+', "", day_of_birth)

    # 8桁未満の入力は'入力エラーを出力'
    if len(day_of_birth) < 8:
        return "入力エラー"
    
    class birthday:
    	year = int(day_of_birth[:4])
    	month_day = int(day_of_birth[4:8])

    class today:
    	today = dt.date.today().strftime("%Y%m%d")
    	year = int(today[:4])
    	month_day = int(today[4:8])

    # 1. 今年から誕生年を引く
    age = today.year - birthday.year

    # 2. 今年の誕生日を迎えていなければ、-1
    if (today.month_day - birthday.month_day) < 0:
    	age = age - 1
    
    # 〇〇代出力
    if  int(age) < 10: # 10歳以上
    	print('10代未満')
    elif int(age) > 100: #100歳以上
        print('100歳以上')
    else: #0 < age < 100
        print(f'{str(age)[:1]}0代')

    return age
