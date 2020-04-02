import os

import pandas as pd

from salespy import SalesforceClass, EpicorClass

# login to Salesforce & get credentials
sf = SalesforceClass(
    username='bko@fs-elliott.com.test', sandbox='test',
    org_id='00D7A0000009Kmw'
)
creds = sf.login()

# get most recent Tran Num for filtering Epicor report
last_tran_num = sf.get_last_tran(creds=creds)
print(f'--- Last SFDC Tran Num Extracted: {last_tran_num} ---')

# get Epicor file
# ----- TODO: Loop through files and delete ----- #
directory = (
    '/Users/beekayokay/OneDrive/Projects/'
    'Salesforce Epicor Integration/'
)
file_name = 'test_bookings_df_2.xlsx'
epicor = EpicorClass()
bookings_df = epicor.bookings_to_df(os.path.join(directory, file_name))

# remove old Tran Nums
old_trans = bookings_df['Name'] <= last_tran_num
bookings_df = bookings_df[~old_trans]

# add fo and asset
bookings_df = sf.add_sn_fo(creds=creds, df=bookings_df)
bookings_df = sf.add_sn_asset(creds=creds, df=bookings_df)
print(f'--- Bookings Data Size: {bookings_df.size} ---')

if bookings_df.size > 0:
    # get tuples of Order Num Ln
    order_num_ln = tuple(bookings_df['Order_Num_Ln__c'])

    # get Salesforce records to append
    sfdc_df = sf.get_epicor_trans(creds=creds, order_num_ln=order_num_ln)
else:
    sfdc_df = pd.DataFrame()
print(f'--- Old SFDC Data Size: {sfdc_df.size} ---')

# merge DataFrames
merged_df = epicor.merge_dfs(bookings_df, sfdc_df)
print(f'--- Merged Data Size: {merged_df.size} ---')

# upsert data
if merged_df.size > 0:
    sf.upsert_data(
        creds=creds, object_api='Epicor_Transaction__c',
        ex_id='Order_Num_Ln__c', df=merged_df
    )
print('--- DONE! ---')
