import os

from salespy import SalesforceClass, EpicorClass

# get Epicor file
directory = (
    '/Users/beekayokay/OneDrive/Projects/'
    'Salesforce Epicor Integration/'
)
file_name = 'test_bookings_df.xlsx'

# call Epicor Class and assign DataFrame
epicor = EpicorClass()
bookings_df = epicor.bookings_to_df(os.path.join(directory, file_name))

bookings_df.to_excel('test_bookings.xlsx')
