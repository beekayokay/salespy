import os
import shutil

import pandas as pd
from salespy import SalesforceClass, EpicorClass

# login to Salesforce & get credentials
sf = SalesforceClass(
    username='bko@fs-elliott.com.test', sandbox='test',
    org_id='00D7A0000009Kmw'
)
creds = sf.login()
print(f'--- Logged Into Salesforce ---')

# epicor file directory
directory = (
    '/Users/beekayokay/OneDrive/Projects/'
    'Salesforce Epicor Integration/E10 Reports To Upload'
)

# loop through epicor files and merge
count = 0
epicor = EpicorClass()
bookings_df = pd.DataFrame()
for each in os.listdir(directory):
    temp_df = pd.DataFrame()
    if each == '.DS_Store':
        continue
    try:
        temp_df = epicor.bookings_to_df(os.path.join(directory, each))
        bookings_df = bookings_df.append(temp_df, ignore_index=True)
    except IsADirectoryError:
        continue
bookings_df.sort_values(by=['Name'], ascending=True, inplace=True)
bookings_df.drop_duplicates(inplace=True, keep='first')

# get most recent Tran Num for filtering Epicor report
last_tran_num = sf.get_last_tran(creds=creds)
print(f'--- Got Last Tran Num: {last_tran_num} ---')

# remove existing SFDC Tran Nums
old_trans = bookings_df['Name'] <= last_tran_num
bookings_df = bookings_df[~old_trans]
print(f'--- Removed Existing Epicor Data: {bookings_df.size} ---')

# get tuples of Order Num Ln
order_num_ln = tuple(bookings_df['Order_Num_Ln__c'])
print(f'--- Compiled Order Num Ln ---')

# get Salesforce records to append
sfdc_df = sf.get_epicor_trans(creds=creds, order_num_ln=order_num_ln)
print(f'--- Got Matching SFDC Data: {sfdc_df.size} ---')

# add fo and asset
bookings_df = sf.add_sn_fo(creds=creds, df=bookings_df)
bookings_df = sf.add_sn_asset(creds=creds, df=bookings_df)
print(f'--- Added FO & SN: {bookings_df.size} ---')

# add cp and sales lead
bookings_df = sf.add_code_ref(creds=creds, df=bookings_df)
print(f'--- Added CP & SL: {bookings_df.size} ---')

# merge DataFrames
merged_df = epicor.merge_dfs(bookings_df, sfdc_df)
print(f'--- Merged DataFrames: {merged_df.size} ---')

# upsert data
if merged_df.size > 0:
    sf.upsert_data(
        creds=creds, object_api='Epicor_Transaction__c',
        ex_id='Order_Num_Ln__c', df=merged_df
    )

# move upserted files
for each in os.listdir(directory):
    if each == '.DS_Store' or each == 'Uploaded':
        continue
    shutil.move(
        os.path.join(directory, each),
        os.path.join(f'{directory}/Uploaded', each)
    )

print('--- DONE! ---')
