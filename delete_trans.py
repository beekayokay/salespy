from datetime import datetime
import getpass
import json
import random

import pandas as pd
from simple_salesforce import Salesforce

username = 'bko@fs-elliott.com.test'
sandbox = 'test'
org_id = '00D7A0000009Kmw'
password = getpass.getpass(prompt='Salesforce Password: ')

sf = Salesforce(
    username=username, password=password,
    organizationId=org_id, sandbox=sandbox
)

password = '%030x' % random.randrange(16**30)


query_call = (
    "SELECT Id FROM Epicor_Transaction__c"
)
raw_query = sf.bulk.Epicor_Transaction__c.query(query_call)
query_df = pd.DataFrame(raw_query).iloc[:, 1:]
df_tuple = query_df.itertuples(index=False)

bulk_data = []
count_records = 0
count_chars = 0
count_rows = 0
start_time = datetime.now()
time_check = datetime.now()

for each in df_tuple:
    if (datetime.now() - time_check).seconds > 15:
        print(
            f'{count_rows} rows have been processed. '
            f'{round((datetime.now() - start_time).seconds/60, 2)} '
            'minutes have passed'
        )
        time_check = datetime.now()

    if count_records >= 5000 or count_chars >= 5000000:
        sf.bulk.Epicor_Transaction__c.delete(bulk_data)
        bulk_data = []
        count_records = 0
        count_chars = 0
    each_dict = each._asdict()
    bulk_data.append(each_dict)
    count_records += 1
    count_chars += len(json.dumps(each_dict))
    count_rows += 1
sf.bulk.Epicor_Transaction__c.delete(bulk_data)

print(
    f'{count_rows} rows have been processed. '
    f'{round((datetime.now() - start_time).seconds/60, 2)} '
    'minutes have passed'
)
