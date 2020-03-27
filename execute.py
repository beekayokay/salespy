import os

from salespy import SalesforceClass, EpicorClass

# login to Salesforce
sf = SalesforceClass(
    username='bko@fs-elliott.com.test', sandbox='test',
    org_id='00D7A0000009Kmw'
)

creds = sf.login()

# get most recent Tran Num
last_tran_num = sf.get_last_tran(creds=creds)

# get Epicor file
directory = (
    '/Users/beekayokay/OneDrive/Projects/'
    'Salesforce Epicor Integration/'
)
file_name = 'test_bookings_df_2.xlsx'

# call Epicor Class and assign DataFrame
epicor = EpicorClass()
bookings_df = epicor.bookings_to_df(os.path.join(directory, file_name))
print(bookings_df)
print('-'*100)

# remove old Tran Nums
old_trans = bookings_df['Name'] <= last_tran_num
bookings_df = bookings_df[~old_trans]
print(bookings_df)
print('-'*100)

# get tuples of Order Num Ln
order_num_ln = tuple(bookings_df['Order_Num_Ln__c'])
print(order_num_ln)
print('-'*100)

# get Salesforce records to append
sfdc_df = sf.get_epicor_trans(creds=creds, order_num_ln=order_num_ln)
print(sfdc_df)
print('-'*100)

# merge DataFrames
merged_df = epicor.merge_dfs(bookings_df, sfdc_df)
print(merged_df)
print('-'*100)
