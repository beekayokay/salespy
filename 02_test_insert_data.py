from datetime import datetime
import getpass
import json

import pandas as pd
from simple_salesforce import Salesforce, SalesforceLogin

# login credentials
username = 'bko@fs-elliott.com.backup'
password = getpass.getpass(prompt='Password: ')
security_token = 'q3hiTl2zYMZ28EPc62JZljDXT'
sandbox = 'backup'

# time checks
start_time = datetime.now()
time_check = datetime.now()

# get session to start job
session_id, instance = SalesforceLogin(username, password, security_token=security_token, sandbox=sandbox)
sf = Salesforce(instance=instance, session_id=session_id)

# csv to DataFrame, then to iterable tuple
csv_df = pd.read_csv('acc_insert_test.csv')
csv_tuple = csv_df.itertuples(index=False)

# initalize for Bulk API Limits
bulk_data = []
count_records = 0
count_chars = 0
count_rows = 0

for each in csv_tuple:
    if (datetime.now() - time_check).seconds > 15:
        print(f'{count_rows} rows have been processed. {round((datetime.now() - start_time).seconds/60, 2)} minutes have passed')
        time_check = datetime.now()

    # Max of 10,000 records and 10,000,000 characters per Salesforce Bulk API Limits
    if count_records >= 9000 or count_chars >= 9000000:
        sf.bulk.Account.insert(bulk_data)
        bulk_data = []
        count_records = 0
        count_chars = 0
    each_dict = each._asdict()
    bulk_data.append(each_dict)
    count_records += 1
    count_chars += len(json.dumps(each_dict))
    count_rows += 1
sf.bulk.Account.insert(bulk_data)

# query = sf.bulk.Account.query("SELECT Id, Name FROM Account ORDER BY CreatedDate DESC LIMIT 10")
# query_df = pd.DataFrame(query).iloc[:, 1:]
# print(query_df)


# print timestamp
end_time = datetime.now()
time_delta = end_time - start_time
time_delta_min = int(time_delta.seconds/60)
time_delta_sec = time_delta.seconds - (time_delta_min * 60)
print(f'Finished in {time_delta_min}m{time_delta_sec}s')