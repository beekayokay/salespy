from datetime import datetime
import os

import pandas as pd

start_time = datetime.now()

directory = """/Users/beekayokay/OneDrive/Projects/Salesforce Epicor Integration\
/Epicor Reports"""

bookings_df = pd.DataFrame()
count = 0

for each in os.listdir(directory):
    if each == '.DS_Store':
        continue
    count += 1
    data = pd.read_excel(os.path.join(directory, each), index_col='Tran Num')
    bookings_df = bookings_df.append(data)

bookings_df.sort_index(ascending=True, inplace=True)

print(count)
print(bookings_df)

bookings_df.to_excel('/Users/beekayokay/OneDrive/Projects/Salesforce Epicor Integration/test_bookings_df.xlsx')

end_time = datetime.now()
time_delta = end_time - start_time
time_delta_min = int(time_delta.seconds/60)
time_delta_sec = time_delta.seconds - (time_delta_min * 60)
print(f'Finished in {time_delta_min}m{time_delta_sec}s')
