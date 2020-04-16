from datetime import datetime
import getpass
import json
import random

import numpy as np
import pandas as pd
from simple_salesforce import Salesforce, SalesforceLogin


SFDC_LIST = [
    'OrderNum__c', 'OrderLn__c', 'Name', 'Book_Date__c', 'BookType__c',
    'OrderRel__c', 'Prod_Code__c', 'SN__c', 'Order_Line_Amt__c', 'PO_Num__c',
    'Need_By_Date__c', 'Quote_Num__c', 'Quote_Line__c',
    'Sales_Representative__c', 'Salesperson__c',
    'Primary_Salesperson_Origin__c', 'Sales_Director_Origin__c',
    'Regional_Sales_Manager_Origin__c', 'Cust_ID__c', 'Customer_Name__c',
    'Ultimate_User__c', 'Bill_To_Name__c', 'Bill_To_Address_1__c',
    'Bill_To_Address_2__c', 'Bill_To_Address_3__c', 'Bill_To_City__c',
    'Bill_To_State__c', 'Bill_To_Zip__c', 'Bill_To_Country__c',
    'Ship_To_Num__c', 'Ship_To_Name__c', 'Ship_To_Address_1__c',
    'Ship_To_Address_2__c', 'Ship_To_Address_3__c', 'Ship_To_City__c',
    'Ship_To_State__c', 'Ship_To_Zip__c', 'Ship_To_Country__c', 'Packager__c',
    'Market_Segment__c', 'Payment_Terms__c', 'OrderHed_FOB__c', 'SysRowID__c',
    'Order_Num_Ln__c', 'Factory_Order__c', 'Asset__c', 'Sales_Lead__c',
    'Channel_Partner__c'
]

EPICOR_TO_SFDC_DICT = {
    'Tran Num': 'Name', 'Book Date': 'Book_Date__c', 'BookType': 'BookType__c',
    'OrderNum': 'OrderNum__c', 'OrderLn': 'OrderLn__c',
    'OrderRel': 'OrderRel__c', 'Prod Code': 'Prod_Code__c', 'SN': 'SN__c',
    'Order Line Amt': 'Order_Line_Amt__c', 'PO Num': 'PO_Num__c',
    'Need By Date': 'Need_By_Date__c', 'Quote Num': 'Quote_Num__c',
    'Quote Line': 'Quote_Line__c',
    'Sales Representative': 'Sales_Representative__c',
    'Salesperson': 'Salesperson__c',
    'Primary Salesperson Origin': 'Primary_Salesperson_Origin__c',
    'Sales Director Origin': 'Sales_Director_Origin__c',
    'Regional Sales Manager Origin': 'Regional_Sales_Manager_Origin__c',
    'Cust. ID': 'Cust_ID__c', 'Customer Name': 'Customer_Name__c',
    'Ultimate User': 'Ultimate_User__c', 'Bill To Name': 'Bill_To_Name__c',
    'Bill To  Address': 'Bill_To_Address_1__c',
    'Bill To Address2': 'Bill_To_Address_2__c',
    'Bill To Address3': 'Bill_To_Address_3__c',
    'Bill To  Address.1': 'Bill_To_City__c',
    'Bill To State': 'Bill_To_State__c', 'Bill To Zip': 'Bill_To_Zip__c',
    'Bill To Country': 'Bill_To_Country__c', 'ShipToNum': 'Ship_To_Num__c',
    'ShipTo.Name': 'Ship_To_Name__c',
    'ShipTo.Address1': 'Ship_To_Address_1__c',
    'ShipTo.Address2': 'Ship_To_Address_2__c',
    'ShipTo.Address3': 'Ship_To_Address_3__c',
    'ShipTo.City': 'Ship_To_City__c', 'ShipTo.State': 'Ship_To_State__c',
    'ShipTo.ZIP': 'Ship_To_Zip__c', 'ShipTo.Country': 'Ship_To_Country__c',
    'Packager': 'Packager__c', 'Market Segment': 'Market_Segment__c',
    'Payment Terms': 'Payment_Terms__c', 'OrderHed.FOB': 'OrderHed_FOB__c',
    'SysRowID': 'SysRowID__c'
}

EPICOR_AGG_DICT = {
    'Name': 'last', 'Book_Date__c': 'last', 'BookType__c': 'last',
    'OrderRel__c': 'max', 'Prod_Code__c': 'last', 'SN__c': 'first',
    'Order_Line_Amt__c': 'sum', 'PO_Num__c': 'first',
    'Need_By_Date__c': 'last', 'Quote_Num__c': 'first',
    'Quote_Line__c': 'first', 'Sales_Representative__c': 'first',
    'Salesperson__c': 'first', 'Primary_Salesperson_Origin__c': 'first',
    'Sales_Director_Origin__c': 'first',
    'Regional_Sales_Manager_Origin__c': 'first', 'Cust_ID__c': 'first',
    'Customer_Name__c': 'first', 'Ultimate_User__c': 'first',
    'Bill_To_Name__c': 'first', 'Bill_To_Address_1__c': 'first',
    'Bill_To_Address_2__c': 'first', 'Bill_To_Address_3__c': 'first',
    'Bill_To_City__c': 'first', 'Bill_To_State__c': 'first',
    'Bill_To_Zip__c': 'first', 'Bill_To_Country__c': 'first',
    'Ship_To_Num__c': 'first', 'Ship_To_Name__c': 'first',
    'Ship_To_Address_1__c': 'first', 'Ship_To_Address_2__c': 'first',
    'Ship_To_Address_3__c': 'first', 'Ship_To_City__c': 'first',
    'Ship_To_State__c': 'first', 'Ship_To_Zip__c': 'first',
    'Ship_To_Country__c': 'first', 'Packager__c': 'first',
    'Market_Segment__c': 'first', 'Payment_Terms__c': 'first',
    'OrderHed_FOB__c': 'first', 'SysRowID__c': 'last'
}

SFDC_EPICOR_AGG_DICT = {
    'Name': 'last', 'Book_Date__c': 'last', 'BookType__c': 'last',
    'OrderRel__c': 'max', 'Prod_Code__c': 'last', 'SN__c': 'first',
    'Order_Line_Amt__c': 'sum', 'PO_Num__c': 'first',
    'Need_By_Date__c': 'last', 'Quote_Num__c': 'first',
    'Quote_Line__c': 'first', 'Sales_Representative__c': 'first',
    'Salesperson__c': 'first', 'Primary_Salesperson_Origin__c': 'first',
    'Sales_Director_Origin__c': 'first',
    'Regional_Sales_Manager_Origin__c': 'first', 'Cust_ID__c': 'first',
    'Customer_Name__c': 'first', 'Ultimate_User__c': 'first',
    'Bill_To_Name__c': 'first', 'Bill_To_Address_1__c': 'first',
    'Bill_To_Address_2__c': 'first', 'Bill_To_Address_3__c': 'first',
    'Bill_To_City__c': 'first', 'Bill_To_State__c': 'first',
    'Bill_To_Zip__c': 'first', 'Bill_To_Country__c': 'first',
    'Ship_To_Num__c': 'first', 'Ship_To_Name__c': 'first',
    'Ship_To_Address_1__c': 'first', 'Ship_To_Address_2__c': 'first',
    'Ship_To_Address_3__c': 'first', 'Ship_To_City__c': 'first',
    'Ship_To_State__c': 'first', 'Ship_To_Zip__c': 'first',
    'Ship_To_Country__c': 'first', 'Packager__c': 'first',
    'Market_Segment__c': 'first', 'Payment_Terms__c': 'first',
    'OrderHed_FOB__c': 'first', 'SysRowID__c': 'last',
    'Order_Num_Ln__c': 'last', 'Factory_Order__c': 'first',
    'Asset__c': 'first', 'Sales_Lead__c': 'first',
    'Channel_Partner__c': 'first'
}


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

        if last_tran_df.size > 0:
            return last_tran_df.iloc[0][0]
        else:
            return 0

    def get_epicor_trans(self, creds, order_num_ln):
        sfdc_fields = SFDC_LIST

        if len(order_num_ln) == 0:
            query_df = pd.DataFrame()
        else:
            if len(order_num_ln) == 1:
                order_num_ln = order_num_ln[0]
            query_call = (
                f"SELECT {', '.join(map(str, sfdc_fields))} "
                "FROM Epicor_Transaction__c "
                f"WHERE Order_Num_Ln__c IN {order_num_ln}"
            )

            try:
                query_df = self.bulk_query(
                    creds=creds, object_api='Epicor_Transaction__c',
                    query_call=query_call
                )
            except IndexError:
                query_df = pd.DataFrame()

        return query_df

    def add_sn_fo(self, creds, df):
        sn = df['SN__c'].drop_duplicates()
        sn.dropna(inplace=True)
        sn_tuple = tuple(sn)

        fo_df = pd.DataFrame(columns=['Id', 'Name'])
        sn_temp_list = []

        if len(sn_tuple) > 0:
            if len(sn_tuple) == 1:
                sn_tuple = sn_tuple[0]

            for num, each in enumerate(sn_tuple):
                sn_temp_list.append(each)
                if len(str(sn_temp_list)) < 50000 and num != len(sn_tuple)-1:
                    continue
                else:
                    fo_query = (
                        "SELECT Id, Name FROM Factory_Order__c "
                        f"WHERE Name IN {tuple(sn_temp_list)}"
                    )
                    fo_df_temp = self.bulk_query(
                        creds=creds, object_api='Factory_Order__c',
                        query_call=fo_query
                    )
                    fo_df = fo_df.append(
                        fo_df_temp, ignore_index=True
                    )
                    fo_df_temp = pd.DataFrame()
                    sn_temp_list = []

        fo_df.rename(
            columns={
                'Id': 'Factory_Order__c',
                'Name': 'SN__c'
            },
            inplace=True
        )

        df = df.merge(fo_df, how='left', on='SN__c')

        return df

    def add_sn_asset(self, creds, df):
        sn = df['SN__c'].drop_duplicates()
        sn.dropna(inplace=True)
        sn_tuple = tuple(sn)

        asset_df = pd.DataFrame(columns=['Id', 'Name'])
        sn_temp_list = []

        if len(sn_tuple) > 0:
            if len(sn_tuple) == 1:
                sn_tuple = sn_tuple[0]

            for num, each in enumerate(sn_tuple):
                sn_temp_list.append(each)
                if len(str(sn_temp_list)) < 50000 and num != len(sn_tuple)-1:
                    continue
                else:
                    asset_query = (
                        "SELECT Id, Name FROM Asset "
                        f"WHERE Name IN {tuple(sn_temp_list)}"
                    )
                    asset_df_temp = self.bulk_query(
                        creds=creds, object_api='Asset',
                        query_call=asset_query
                    )
                    asset_df = asset_df.append(
                        asset_df_temp, ignore_index=True
                    )
                    asset_df_temp = pd.DataFrame()
                    sn_temp_list = []

        asset_df.rename(
            columns={
                'Id': 'Asset__c',
                'Name': 'SN__c'
            },
            inplace=True
        )

        df = df.merge(asset_df, how='left', on='SN__c')

        return df

    def add_code_ref(self, creds, df):

        sp_code = df['Primary_Salesperson_Origin__c'].drop_duplicates()
        sp_code.dropna(inplace=True)
        sp_tuple = tuple(sp_code)

        rep_code = df['Salesperson__c'].drop_duplicates()
        rep_code.dropna(inplace=True)
        rep_tuple = tuple(rep_code)

        user_df = pd.DataFrame(columns=['Id', 'Salescode__c'])
        acc_df = pd.DataFrame(
            columns=[
                'Id', 'FS_Elliott_Sales_Lead__c', 'Distributor_Agreement__c',
                'Sales_Representative_Agreement__c'
            ]
        )
        user_temp_list = []
        acc_temp_list = []

        if len(sp_tuple) > 0:
            if len(sp_tuple) == 1:
                sp_tuple = sp_tuple[0]
            for num, each in enumerate(sp_tuple):
                user_temp_list.append(each)
                if (
                    len(str(user_temp_list)) < 50000
                    and
                    num != len(sp_tuple)-1
                ):
                    continue
                else:
                    user_query = (
                        "SELECT Id, Salescode__c "
                        "FROM User "
                        "WHERE "
                        f"(Salescode__c IN {tuple(user_temp_list)} AND "
                        "UserType = 'Standard')"
                    )
                    user_df_temp = self.bulk_query(
                        creds=creds, object_api='User',
                        query_call=user_query
                    )
                    user_df = user_df.append(
                        user_df_temp, ignore_index=True
                    )
                    user_df_temp = pd.DataFrame()
                    user_temp_list = []

        if len(rep_tuple) > 0:
            if len(rep_tuple) == 1:
                rep_tuple = rep_tuple[0]
            for num, each in enumerate(rep_tuple):
                acc_temp_list.append(each)
                if (
                    len(str(acc_temp_list)) < 50000
                    and
                    num != len(rep_tuple)-1
                ):
                    continue
                else:
                    acc_query = (
                        "SELECT Id, FS_Elliott_Sales_Lead__c, "
                        "Distributor_Agreement__c, "
                        "Sales_Representative_Agreement__c "
                        "FROM Account "
                        "WHERE "
                        f"(Distributor_Agreement__c IN {tuple(acc_temp_list)} "
                        "OR "
                        "Sales_Representative_Agreement__c IN "
                        f"{tuple(acc_temp_list)})"
                    )
                    acc_df_temp = self.bulk_query(
                        creds=creds, object_api='Account',
                        query_call=acc_query
                    )
                    acc_df = acc_df.append(
                        acc_df_temp, ignore_index=True
                    )
                    acc_df_temp = pd.DataFrame()
                    acc_temp_list = []

        dist_df = acc_df[
            [
                'Id', 'FS_Elliott_Sales_Lead__c',
                'Distributor_Agreement__c'
            ]
        ].copy()
        rep_df = acc_df[
            [
                'Id', 'FS_Elliott_Sales_Lead__c',
                'Sales_Representative_Agreement__c'
            ]
        ].copy()
        user_df.rename(
            columns={
                'Id': 'Sales_Lead__c',
                'Salescode__c': 'Primary_Salesperson_Origin__c'
            },
            inplace=True
        )

        dist_df.rename(
            columns={
                'Id': 'Channel_Partner__c',
                'FS_Elliott_Sales_Lead__c': 'SL_Dist',
                'Distributor_Agreement__c': 'Salesperson__c'
            },
            inplace=True
        )
        rep_df.rename(
            columns={
                'Id': 'CP_Rep',
                'FS_Elliott_Sales_Lead__c': 'SL_Rep',
                'Sales_Representative_Agreement__c': 'Salesperson__c'
            },
            inplace=True
        )

        df = df.merge(user_df, how='left', on='Primary_Salesperson_Origin__c')
        df = df.merge(dist_df, how='left', on='Salesperson__c')
        df = df.merge(rep_df, how='left', on='Salesperson__c')

        sl_inds = df['Sales_Lead__c'].isnull()
        df.loc[sl_inds, 'Sales_Lead__c'] = df.loc[sl_inds, 'SL_Dist']
        sl_inds = df['Sales_Lead__c'].isnull()
        df.loc[sl_inds, 'Sales_Lead__c'] = df.loc[sl_inds, 'SL_Rep']
        cp_inds = df['Channel_Partner__c'].isnull()
        df.loc[cp_inds, 'Channel_Partner__c'] = df.loc[cp_inds, 'CP_Rep']

        df = df.drop(columns=['SL_Dist', 'CP_Rep', 'SL_Rep'])

        return df

    def upsert_data(self, creds, object_api, ex_id, df):
        df['Book_Date__c'] = df['Book_Date__c'].dt.strftime('%Y-%m-%d')
        df['Need_By_Date__c'] = df['Need_By_Date__c'].dt.strftime('%Y-%m-%d')
        df['Book_Date__c'] = df['Book_Date__c'].replace(np.nan, '1900-01-01')
        df['Need_By_Date__c'] = df['Need_By_Date__c'].replace(
            np.nan, '1900-01-01'
        )
        df = df.replace(np.nan, '', regex=True)

        df_tuple = df.itertuples(index=False)

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
                getattr(creds.bulk, object_api).upsert(bulk_data, ex_id)
                bulk_data = []
                count_records = 0
                count_chars = 0
            each_dict = each._asdict()
            bulk_data.append(each_dict)
            count_records += 1
            count_chars += len(json.dumps(each_dict))
            count_rows += 1
        getattr(creds.bulk, object_api).upsert(bulk_data, ex_id)

        print(
            f'{count_rows} rows have been processed. '
            f'{round((datetime.now() - start_time).seconds/60, 2)} '
            'minutes have passed'
        )


class EpicorClass:
    def __init__(self):
        pass

    def bookings_to_df(self, read):
        bookings_df = pd.read_excel(
            read,
            dtype={
                # 'Book Date': 'datetime64[D]',
                # 'Need By Date': 'datetime64[D]',
                'Quote Num': 'object',
                'Quote Line': 'object',
                'SN': 'object'
            }
        )

        bookings_df['Book Date'] = pd.to_datetime(
            bookings_df['Book Date'], errors='coerce'
        )
        bookings_df['Need By Date'] = pd.to_datetime(
            bookings_df['Need By Date'], errors='coerce'
        )

        bookings_df.loc[bookings_df['Quote Num'] == 0, 'Quote Num'] = np.nan
        bookings_df.loc[bookings_df['Quote Line'] == 0, 'Quote Line'] = np.nan
        bookings_df = bookings_df.replace('\n', ' ', regex=True)
        bookings_df = bookings_df.replace("'", ' ', regex=True)
        if bookings_df['SN'].count() != 0:
            bookings_df['SN'] = bookings_df['SN'].str.strip()
            bookings_df['SN'] = bookings_df['SN'].str[:254]

        return bookings_df

    def aggregate_df(self, df):
        df.sort_values(
            by=['Tran Num'],
            ascending=True,
            inplace=True
        )

        df.drop_duplicates(
            subset=['Tran Num', 'SysRowID'], inplace=True,
            keep='first'
        )

        df.rename(
            columns=EPICOR_TO_SFDC_DICT,
            inplace=True
        )

        bookings_df_grouped = df.groupby(
            ['OrderNum__c', 'OrderLn__c'],
            as_index=False
        ).agg(EPICOR_AGG_DICT)

        bookings_df_grouped['Order_Num_Ln__c'] = (
            bookings_df_grouped.loc[:, 'OrderNum__c'].astype(str)
            + "-"
            + bookings_df_grouped.loc[:, 'OrderLn__c'].astype(str)
        )

        return bookings_df_grouped

    def merge_dfs(self, df1, df2):
        merged_df = df1.append(df2)
        merged_df = merged_df.astype({'Name': 'int32'})
        merged_df.sort_values(
            by=['Name'],
            ascending=True,
            inplace=True
        )

        df_grouped = merged_df.groupby(
            ['OrderNum__c', 'OrderLn__c'],
            as_index=False
        ).agg(SFDC_EPICOR_AGG_DICT)

        return df_grouped
