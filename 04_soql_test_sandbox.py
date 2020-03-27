from datetime import datetime
import getpass

import pandas as pd
from simple_salesforce import Salesforce, SalesforceLogin

# login credentials
username = 'bko@fs-elliott.com.test'
password = getpass.getpass(prompt='Password: ')
security_token = 'VqgyG78G5EiChLcIA4ZpicgkI'
sandbox = 'test'

# time checks
start_time = datetime.now()

# get session to start job
session_id, instance = SalesforceLogin(
    username, password, security_token=security_token, sandbox=sandbox)
sf = Salesforce(instance=instance, session_id=session_id)


def soql_df(soql_query):
    query = sf.query_all(soql_query)['records']
    query_df = pd.DataFrame(query)
    query_df.drop(columns=['attributes'], inplace=True)
    query_df.set_index('Id', inplace=True)
    return query_df


asset_query = """SELECT Id, Name, Factory_Order__c FROM Asset
    WHERE RecordTypeId = '012i0000000twTk'"""
fo_query = """SELECT Id, Name FROM Factory_Order__c"""

asset_df = soql_df(asset_query)
fo_df = soql_df(fo_query)

df_merge = asset_df.merge(fo_df, left_on='Factory_Order__c', right_on='Id')

print(df_merge)
print(df_merge.info())

# print timestamp
end_time = datetime.now()
time_delta = end_time - start_time
time_delta_min = int(time_delta.seconds/60)
time_delta_sec = time_delta.seconds - (time_delta_min * 60)
print(f'Finished in {time_delta_min}m{time_delta_sec}s')
