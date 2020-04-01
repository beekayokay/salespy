import os

from salespy import SalesforceClass, EpicorClass

# login to Salesforce
sf = SalesforceClass(
    username='bko@fs-elliott.com.test', sandbox='test',
    password='st3313r5', org_id='00D7A0000009Kmw'
)

creds = sf.login()

# get Epicor file
directory = (
    '/Users/beekayokay/OneDrive/Projects/'
    'Salesforce Epicor Integration/'
)
file_name = 'test_upsert.xlsx'

# call Epicor Class and assign DataFrame
epicor = EpicorClass()
bookings_df = epicor.bookings_to_df(os.path.join(directory, file_name))

# upsert data
sf.upsert_data(
    creds=creds, object_api='Epicor_Transaction__c',
    ex_id='Order_Num_Ln__c', df=bookings_df
)
