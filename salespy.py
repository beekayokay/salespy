import getpass
import random

import numpy as np
import pandas as pd
from simple_salesforce import Salesforce, SalesforceLogin


class SalesforceClass:
    def __init__(
        self, username, password=None, sandbox=None, security_token=None,
        org_id='00Di0000000XmEH'
    ):
        self.username = username
        self.password = password
        self.sandbox = sandbox
        self.security_token = security_token
        self.org_id = org_id

    def login(self):
        if self.password is None:
            self.password = getpass.getpass(prompt='Salesforce Password: ')

        if self.sandbox is None:
            if self.security_token is None:
                sf = Salesforce(
                    username=self.username, password=self.password,
                    organizationId=self.org_id
                )
            else:
                session_id, instance = SalesforceLogin(
                    self.username, self.password,
                    security_token=self.security_token
                )
                sf = Salesforce(instance=instance, session_id=session_id)
        else:
            if self.security_token is None:
                sf = Salesforce(
                    username=self.username, password=self.password,
                    organizationId=self.org_id, sandbox=self.sandbox
                )
            else:
                session_id, instance = SalesforceLogin(
                    self.username, self.password,
                    security_token=self.security_token,
                    sandbox=self.sandbox
                )
                sf = Salesforce(instance=instance, session_id=session_id)

        self.password = '%030x' % random.randrange(16**30)
        return sf

    def bulk_query(self, creds, object_api, query_call):
        raw_query = getattr(creds.bulk, object_api).query(query_call)
        query_df = pd.DataFrame(raw_query).iloc[:, 1:]
        return query_df

    def get_last_tran(self, creds):
        last_tran_query = (
            "SELECT Tran_Num__c FROM Epicor_Transaction__c "
            "ORDER BY Tran_Num__c DESC LIMIT 1"
        )

        last_tran_df = self.bulk_query(
            creds=creds, object_api='Epicor_Transaction__c',
            query_call=last_tran_query
        )

        return last_tran_df.iloc[0][0]

    def get_epicor_trans(self, creds, order_num_ln=None):
        sfdc_fields = [
            'OrderNum__c', 'OrderLn__c', 'Name', 'Book_Date__c', 'BookType__c',
            'OrderRel__c', 'Prod_Code__c', 'SN__c', 'Order_Line_Amt__c',
            'PO_Num__c', 'Need_By_Date__c', 'Quote_Num__c', 'Quote_Line__c',
            'Sales_Representative__c', 'Salesperson__c',
            'Primary_Salesperson_Origin__c', 'Sales_Director_Origin__c',
            'Regional_Sales_Manager_Origin__c', 'Cust_ID__c',
            'Customer_Name__c', 'Ultimate_User__c', 'Bill_To_Name__c',
            'Bill_To_Address_1__c', 'Bill_To_Address_2__c',
            'Bill_To_Address_3__c', 'Bill_To_City__c', 'Bill_To_State__c',
            'Bill_To_Zip__c', 'Bill_To_Country__c', 'Ship_To_Num__c',
            'Ship_To_Name__c', 'Ship_To_Address_1__c', 'Ship_To_Address_2__c',
            'Ship_To_Address_3__c', 'Ship_To_City__c', 'Ship_To_State__c',
            'Ship_To_Zip__c', 'Ship_To_Country__c', 'Packager__c',
            'Market_Segment__c', 'Payment_Terms__c', 'OrderHed_FOB__c',
            'SysRowID__c', 'Order_Num_Ln__c'
        ]

        if order_num_ln is None:
            query_call = (
                f"SELECT {', '.join(map(str, sfdc_fields))}"
                "FROM Epicor_Transaction__c"
            )
        else:
            query_call = (
                f"SELECT {', '.join(map(str, sfdc_fields))} "
                "FROM Epicor_Transaction__c "
                f"WHERE Order_Num_Ln__c IN {order_num_ln}"
            )

        query_df = self.bulk_query(
            creds=creds, object_api='Epicor_Transaction__c',
            query_call=query_call
        )

        return query_df


class EpicorClass:
    def __init__(self):
        pass

    def bookings_to_df(self, read):
        bookings_df = pd.read_excel(
            read,
            dtype={
                'Book Date': 'datetime64[D]',
                'Need By Date': 'datetime64[D]',
                'Quote Num': 'object',
                'Quote Line': 'object'
            }
        )

        bookings_df.loc[bookings_df['Quote Num'] == 0, 'Quote Num'] = np.nan
        bookings_df.loc[bookings_df['Quote Line'] == 0, 'Quote Line'] = np.nan

        bookings_df.sort_values(
            by=['Tran Num'],
            ascending=True,
            inplace=True
        )

        bookings_df.rename(
            columns={
                'Tran Num': 'Name',
                'Book Date': 'Book_Date__c',
                'BookType': 'BookType__c',
                'OrderNum': 'OrderNum__c',
                'OrderLn': 'OrderLn__c',
                'OrderRel': 'OrderRel__c',
                'Prod Code': 'Prod_Code__c',
                'SN': 'SN__c',
                'Order Line Amt': 'Order_Line_Amt__c',
                'PO Num': 'PO_Num__c',
                'Need By Date': 'Need_By_Date__c',
                'Quote Num': 'Quote_Num__c',
                'Quote Line': 'Quote_Line__c',
                'Sales Representative': 'Sales_Representative__c',
                'Salesperson': 'Salesperson__c',
                'Primary Salesperson Origin': 'Primary_Salesperson_Origin__c',
                'Sales Director Origin': 'Sales_Director_Origin__c',
                'Regional Sales Manager Origin': 'Regional_Sales_Manager_Origin__c',
                'Cust. ID': 'Cust_ID__c',
                'Customer Name': 'Customer_Name__c',
                'Ultimate User': 'Ultimate_User__c',
                'Bill To Name': 'Bill_To_Name__c',
                'Bill To  Address': 'Bill_To_Address_1__c',
                'Bill To Address2': 'Bill_To_Address_2__c',
                'Bill To Address3': 'Bill_To_Address_3__c',
                'Bill To  Address.1': 'Bill_To_City__c',
                'Bill To State': 'Bill_To_State__c',
                'Bill To Zip': 'Bill_To_Zip__c',
                'Bill To Country': 'Bill_To_Country__c',
                'ShipToNum': 'Ship_To_Num__c',
                'ShipTo.Name': 'Ship_To_Name__c',
                'ShipTo.Address1': 'Ship_To_Address_1__c',
                'ShipTo.Address2': 'Ship_To_Address_2__c',
                'ShipTo.Address3': 'Ship_To_Address_3__c',
                'ShipTo.City': 'Ship_To_City__c',
                'ShipTo.State': 'Ship_To_State__c',
                'ShipTo.ZIP': 'Ship_To_Zip__c',
                'ShipTo.Country': 'Ship_To_Country__c',
                'Packager': 'Packager__c',
                'Market Segment': 'Market_Segment__c',
                'Payment Terms': 'Payment_Terms__c',
                'OrderHed.FOB': 'OrderHed_FOB__c',
                'SysRowID': 'SysRowID__c'
            },
            inplace=True
        )

        bookings_df_grouped = bookings_df.groupby(
            ['OrderNum__c', 'OrderLn__c'],
            as_index=False
        ).agg({
            'Name': 'last',
            'Book_Date__c': 'last',
            'BookType__c': 'last',
            'OrderRel__c': 'max',
            'Prod_Code__c': 'last',
            'SN__c': 'first',
            'Order_Line_Amt__c': 'sum',
            'PO_Num__c': 'first',
            'Need_By_Date__c': 'last',
            'Quote_Num__c': 'first',
            'Quote_Line__c': 'first',
            'Sales_Representative__c': 'first',
            'Salesperson__c': 'first',
            'Primary_Salesperson_Origin__c': 'first',
            'Sales_Director_Origin__c': 'first',
            'Regional_Sales_Manager_Origin__c': 'first',
            'Cust_ID__c': 'first',
            'Customer_Name__c': 'first',
            'Ultimate_User__c': 'first',
            'Bill_To_Name__c': 'first',
            'Bill_To_Address_1__c': 'first',
            'Bill_To_Address_2__c': 'first',
            'Bill_To_Address_3__c': 'first',
            'Bill_To_City__c': 'first',
            'Bill_To_State__c': 'first',
            'Bill_To_Zip__c': 'first',
            'Bill_To_Country__c': 'first',
            'Ship_To_Num__c': 'first',
            'Ship_To_Name__c': 'first',
            'Ship_To_Address_1__c': 'first',
            'Ship_To_Address_2__c': 'first',
            'Ship_To_Address_3__c': 'first',
            'Ship_To_City__c': 'first',
            'Ship_To_State__c': 'first',
            'Ship_To_Zip__c': 'first',
            'Ship_To_Country__c': 'first',
            'Packager__c': 'first',
            'Market_Segment__c': 'first',
            'Payment_Terms__c': 'first',
            'OrderHed_FOB__c': 'first',
            'SysRowID__c': 'last'
        })

        bookings_df_grouped['Order_Num_Ln__c'] = (
            bookings_df_grouped.loc[:, 'OrderNum__c'].astype(str)
            + "-"
            + bookings_df_grouped.loc[:, 'OrderLn__c'].astype(str)
        )

        return bookings_df_grouped

    def merge_dfs(self, df1, df2):
        df1.append(df2)
        df1.astype({'Name': 'int32'})
        df1.sort_values(
            by=['Name'],
            ascending=True,
            inplace=True
        )

        df_grouped = df1.groupby(
            ['OrderNum__c', 'OrderLn__c'],
            as_index=False
        ).agg({
            'Name': 'last',
            'Book_Date__c': 'last',
            'BookType__c': 'last',
            'OrderRel__c': 'max',
            'Prod_Code__c': 'last',
            'SN__c': 'first',
            'Order_Line_Amt__c': 'sum',
            'PO_Num__c': 'first',
            'Need_By_Date__c': 'last',
            'Quote_Num__c': 'first',
            'Quote_Line__c': 'first',
            'Sales_Representative__c': 'first',
            'Salesperson__c': 'first',
            'Primary_Salesperson_Origin__c': 'first',
            'Sales_Director_Origin__c': 'first',
            'Regional_Sales_Manager_Origin__c': 'first',
            'Cust_ID__c': 'first',
            'Customer_Name__c': 'first',
            'Ultimate_User__c': 'first',
            'Bill_To_Name__c': 'first',
            'Bill_To_Address_1__c': 'first',
            'Bill_To_Address_2__c': 'first',
            'Bill_To_Address_3__c': 'first',
            'Bill_To_City__c': 'first',
            'Bill_To_State__c': 'first',
            'Bill_To_Zip__c': 'first',
            'Bill_To_Country__c': 'first',
            'Ship_To_Num__c': 'first',
            'Ship_To_Name__c': 'first',
            'Ship_To_Address_1__c': 'first',
            'Ship_To_Address_2__c': 'first',
            'Ship_To_Address_3__c': 'first',
            'Ship_To_City__c': 'first',
            'Ship_To_State__c': 'first',
            'Ship_To_Zip__c': 'first',
            'Ship_To_Country__c': 'first',
            'Packager__c': 'first',
            'Market_Segment__c': 'first',
            'Payment_Terms__c': 'first',
            'OrderHed_FOB__c': 'first',
            'SysRowID__c': 'last',
            'Order_Num_Ln__c': 'last'
        })

        return df_grouped
