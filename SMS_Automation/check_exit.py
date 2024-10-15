from datetime import datetime, timedelta
import sys

check_today_is_friday = datetime.today().strftime("%A")
print(check_today_is_friday)

if check_today_is_friday == 'Tuesday':
    sys.exit()
else:

    for i in range (0, 15):
        print ("OK")