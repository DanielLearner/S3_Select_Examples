import pandas as pd
from io import StringIO
from utilities import *


class Table_Selector:
    def __init__(self, bucket_name, file_name):
        self.bucket_name = bucket_name
        self.file_name = file_name


    def select_table(self, column_name_list, sql_expression):
        file_str = StringIO(self.__query(sql_expression, "USE"))
        data_table = pd.read_csv(file_str,names=column_name_list)
        return (data_table)

    def get_column_name_list(self):
        sql_expression = "select * from s3object s limit 1"
        column_name_list = self.__query(sql_expression, "NONE").replace("\n", "").split(sep=",")
        return (column_name_list)

    def __query(self, sql_expression, FileHeaderInfo):
        s3 = get_s3_session_client()
        #print (sql_expression)
        r = s3.select_object_content(
            Bucket=self.bucket_name,
            Key=self.file_name,
            ExpressionType='SQL',
            Expression=sql_expression,
            InputSerialization={'CSV': {"FileHeaderInfo": FileHeaderInfo}},
            OutputSerialization={'CSV': {}},
        )
        return self.__parse_records(r)


    def __parse_records(self, r):
        records = []

        # Table Query
        for event in r['Payload']:
            if 'Records' in event:
                records.append(event['Records']['Payload'])
        file_str = ''.join(row.decode('utf-8') for row in records)
        return (file_str)


if __name__ == '__main__':

    ###########################
    ###### EXAMPLES USES ######
    ###########################
    #Init
    bucket_name = 'my_bucket'
    file_name = 'sample_data.csv'
    ts = Table_Selector(bucket_name, file_name)
    column_name_list = ts.get_column_name_list()

    #Example Select Clauses

    #Full Table
    sql_expression = "select * from s3object s"
    df_full_table = ts.select_table(column_name_list, sql_expression)

    #Columns by Name - can also select by position
    sql_expression = "select Company from s3object s"
    df_company = ts.select_table(["Company, Revenue"], sql_expression)

    #Metric Aggrigation
    sql_expression = "select SUM(CAST(\"Revenue\" AS FLOAT)) from s3object s"
    df_sum_of_revenue = ts.select_table(["Revenue"], sql_expression)


    #Example Where Clauses
    #sql_expression = " select * from s3object s where ...

    # For an exact match
    sql_expression = "select * from s3object s where (s.\"Company\" = 'Initech')"
    df_company_is_initech = ts.select_table(column_name_list, sql_expression)

    # Less than and greater than are also supported
    sql_expression = "select * from s3object s where (CAST(s.\"Year\" as FLOAT) < 2013)"
    df_year_less_than_2013 = ts.select_table(column_name_list, sql_expression)

    # In a list of items
    sql_expression = "select * from s3object s where (s.\"Year\" IN ('2010', '2012'))"
    df_year_is_2010_or_2012 = ts.select_table(column_name_list, sql_expression)

    # NOT proceeds the clause and it is all put in parens
    sql_expression = "select * from s3object s where (NOT s.\"Year\" IN ('2010', '2012'))"
    df_year_is_not_2010_or_2012 = ts.select_table(column_name_list, sql_expression)

    # AND goes between (OR can be used the same way as AND)
    sql_expression = "select * from s3object s where ((s.\"Year\" IN ('2010', '2012')) AND (s.\"Company\" = 'Oscorp'))"
    df_year_is_2010_or_2012_and_Oscorp = ts.select_table(column_name_list, sql_expression)

    # % is a wildcard string of any length
    sql_expression = "select * from s3object s where (NOT s.\"Company\" LIKE ('Os%'))"
    df_company_Os = ts.select_table(column_name_list, sql_expression)

    # _ is a wildcard character of length 1
    sql_expression = "select * from s3object s where (s.\"Revenue\" LIKE ('3___'))"
    df_income_in_3k = ts.select_table(column_name_list, sql_expression)

    #Limit Clause Example
    sql_expression = "select * from s3object s LIMIT 3"
    df_first_three_rows = ts.select_table(column_name_list, sql_expression)

    #Operators Reference
    #https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-glacier-select-sql-reference-operators.html
