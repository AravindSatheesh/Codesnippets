class data_transfer(object):
    
    '''A class object for data upload, deletion from a specified oracle table
    
    Attributes:
    dataframe = A pandas dataframe which needs to be uploaded to oracle table
    conn = A connection object to the oracle schema
    oracle_table = The name string of oracle table'''
    
    
    import pandas as pd
    import sys
    import numbers
    import math
    
    def __init__(self,dataframe,conn,oracle_table):
        self.dataframe=dataframe
        self.conn= conn
        self.oracle_table= oracle_table
        
    def data_upload(self):

        '''This function inserts the observation from a pandas dataframe to oracle table
        Parameters:
            data_df= pandas dataframe
            conn= oracle connection.
            ora_tbl= The name string of destination oracle table. Name to be provided should be in form schema.tbl_name
        Notes: Make sure that the pandas dataframe and oracle table has same columns names.'''
        
        import pandas as pd
        import sys
        import numbers
        import math

        #Checking columns compatibility
        table_name=self.oracle_table.split(".")[1]
        schema=self.oracle_table.split(".")[0]
        col_data= pd.read_sql_query("select COLUMN_NAME,DATA_TYPE,DATA_LENGTH,DEFAULT_LENGTH from ALL_TAB_COLUMNS where table_name = '{tbl}' and owner='{schem}'".format(tbl=table_name,schem=schema.upper()),self.conn)
        
#         for col in col_data['COLUMN_NAME']:
#             if col not in self.dataframe.columns:
#                 if math.isnan(col_data[col_data['COLUMN_NAME'] ==col]['DEFAULT_LENGTH']) ==True:
#                     print("Variable '{}' not found in DataFrame! Please check".format(col))
#                     return

        
#         for i in range(0,len(col_data['COLUMN_NAME'])):
#             for j in range(0,len(self.dataframe.columns)):
#                 if col_data['COLUMN_NAME'].iloc[i] == self.dataframe.columns[j]:
#                     if col_data['DATA_TYPE'].iloc[i] in ['VARCHAR2','VARCHAR','CHAR','DATE']:
#                        self.dataframe[self.dataframe.columns[j]]=self.dataframe[self.dataframe.columns[j]].astype('object')
#                     elif col_data['DATA_TYPE'].iloc[i] in ['NUMBER']:
#                         if not isinstance(self.dataframe[self.dataframe.columns[j]][self.dataframe[self.dataframe.columns[j]].isnull()==False].index[0], numbers.Number):
#                             if '.' in self.dataframe[self.dataframe.columns[j]][self.dataframe[self.dataframe.columns[j]].isnull()==False].index[0]:
#                                 self.dataframe[self.dataframe.columns[j]]=self.dataframe[self.dataframe.columns[j]].astype('float')
#                             else:
#                                 self.dataframe[self.dataframe.columns[j]]=self.dataframe[self.dataframe.columns[j]].astype('int64')

        self.dataframe = self.dataframe.where((pd.notnull(self.dataframe)), None)
        insert_query = '''INSERT INTO {ora_tbl} {col_names} VALUES('''
        col_names = "("
        for i in self.dataframe.columns:
            col_names += str(i) + ","
        col_names = col_names[:len(col_names)-1] + ")"
        insert_query = insert_query.format(ora_tbl=self.oracle_table,col_names=col_names)
        for i in range(1, len(self.dataframe.columns)+1):
            insert_query += ':' + str(i) + ', '
        insert_query = insert_query[:len(insert_query)-2] + ')'
        rows = [tuple(x) for x in self.dataframe.values]
        cur = self.conn.cursor()
        cur.executemany(insert_query, rows)
        self.conn.commit()
        print("Data has been inserted successfully in table : {tbl}"'\n' "Number of observations inserted : {cnt}".format(tbl=self.oracle_table,cnt=self.dataframe.shape[0]))
        
        
        
    def data_delete(self):
        
        '''This function deletes the observations from a oracle table
        Parameters:
            conn= oracle connection.
            oracle_table= The name string of destination oracle table . Name to be provided should be in form schema.tbl_name'''
        
        import pandas as pd
        
        info_query= "select count(*) from {or_tbl}".format(or_tbl=self.oracle_table)
        rows_cnt = pd.read_sql_query(info_query,self.conn)
        
        delete_query= '''Delete from {or_tbl}'''.format(or_tbl=self.oracle_table)
#         print(delete_query)
        
        cur = self.conn.cursor()
        cur.execute(delete_query)
        self.conn.commit()
        
        print("Data has been deleted successfully from the table : {tbl}"'\n' "Number of observations deleted : {cnt}".format(tbl=self.oracle_table,cnt=rows_cnt['COUNT(*)'][0]))
        
        
    def table_dtypes(self):
        
        '''This function will give variable names,datatypes of the provided oracle table
        Parameters:
            conn = oracle connection
            oracle_table= The name string of destination oracle table . Name to be provided should be in form schema.tbl_name'''
        
        import pandas as pd
        
        table_name=self.oracle_table.split(".")[1]
        schema=self.oracle_table.split(".")[0]
        col_data= pd.read_sql_query("select COLUMN_NAME,DATA_TYPE,DATA_LENGTH from ALL_TAB_COLUMNS where table_name = '{tbl}' and owner='{schem}'".format(tbl=table_name,schem=schema.upper()),self.conn)
        
        return col_data
    
    
    def table_info(self):        
        
        '''This function will give count of observations present in oracle table
        Parameters:
            conn = oracle connection
            oracle_table= The name string of oracle table .'''
        
        import pandas as pd
        
        
        info_query= "select count(*) from {or_tbl}".format(or_tbl=self.oracle_table)
        rows_cnt = pd.read_sql_query(info_query,self.conn)
        
        print("Number of observations in {tbl} is : {cnt}".format(tbl=self.oracle_table,cnt=rows_cnt['COUNT(*)'][0]))
        
        return None
        
        
    def create_table(self):        
        '''This function will create oracle table per dataframe passed
        Parameters:
            conn = oracle connection
            oracle_table= The name string of oracle table .'''
        
        my_dtypes = []
        query_columns = ''
        for k, v in zip(self.dataframe.dtypes.index, self.dataframe.dtypes):
            k = k.upper()
            query_columns += ' ' + str(k)
            if v=='object':
                query_columns += ' VARCHAR (2000),'
            elif v=='float64':
                query_columns += ' FLOAT,'
            elif v=='int64':
                query_columns += ' INTEGER,'
            elif v=='bool':
                query_columns += ' BOOLEAN,'
            elif v=='datetime64[ns]':
                query_columns += ' DATE,'
        query_columns = query_columns[:len(query_columns)-1]
        
        create_query = "CREATE TABLE {or_tbl} ({query_columns})".format(or_tbl=self.oracle_table, query_columns= query_columns)
        curr = self.conn.cursor()
        curr.callproc('CDW_SHR.CX_ORACLE_EXCEPTIONS', (create_query,))
        curr.close()

        print("TABLE {tbl} created!!".format(tbl=self.oracle_table))
        print("Please check the table structure to ensure!")
            
        return None
        
    def drop_table(self):
        '''This function will drop oracle table
        Parameters:
            conn = oracle connection
            oracle_table= The name string of oracle table .'''
        
        drop_query = "DROP TABLE {or_tbl}".format(or_tbl=self.oracle_table)
        
        curr = self.conn.cursor()
        curr.callproc('CDW_SHR.CX_ORACLE_EXCEPTIONS', (drop_query,))
        curr.close()

        print("TABLE {tbl} droped!!".format(tbl=self.oracle_table))
                
        return None


        
        