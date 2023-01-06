import json
import time
import cdc_load_to_transform_config as config
from cdc_load_to_transform_connect import snowflake_connector
from snowflake.connector import ProgrammingError
from time import perf_counter
import json

target_schema=config.target_schema
source_schema=config.source_schema
account=config.account
warehouse=config.warehouse
database=config.database
measure =config.measure
offset=config.offset
period=config.period
display=config.display_progress

SF_CONNECTION = snowflake_connector()

class CDCLoadToTransform:

    TRANSACTIONS = []
    total_trans = 0
    WIERD_TABLES = []

    def __init__(self, sf) -> None:
        self.SF = sf
        self.FAILED = 0
        self.current_partition = 1

    def build_execute_merge(self, tablename, target_table, tablecolumns, key_column, target_columns):     
        """ Build a list of transactions to be processed"""
        column_list = str(tablecolumns).split(",")
        statement = self.build_merge(tablename, target_table, column_list, key_column, target_columns)
        response = self.execute_merge(statement,tablename)
        if display:
            print(f"ins:  {perf_counter()} {format(response)}")
        return True

    def execute_merge(self,stmt,tablename):
        """ Execute Snowflake SQL Statments"""
        response = ""
        try:
            self.SF['cur'].execute(stmt)
            response = self.SF['cur'].fetchall()
        except ProgrammingError as err:
            print(f'Programming Error: {format(err)} table: {tablename}')
            self.WIERD_TABLES.append(tablename)
            print(stmt)
        except Exception as e:
            print(f"--------- Exception ------- {format(e)} ")
        return response

    def build_merge(self, tablename, target_table, column_list, key_column, target_columns):

        """ Build the merge statements to apply the STAGE data """
        ignore_cols = ['delete_flag','cdc_load_time']
        select_cols = ""
        target_cols = ""
        update_clause = ""
        on_clause = ""
        val_clause = ""
        col_clause = ""
        for col in column_list:
            if str(col).lower().strip() not in ignore_cols:
                select_cols += f"""{str(col).upper()},""" 

        for col in target_columns:
            if str(col).lower().strip() not in ignore_cols:
                target_cols += f"""{str(col).upper()},""" 

        for col in column_list:
            if str(col).lower().strip() not in ignore_cols:
                update_clause += f"""t1.{str(col).lower()} = t2.{str(col).lower()},""" 

        on_clause = []
        li = list(key_column.split(","))
        for col in li:
            on_clause.append(f"""t1.{str(col)} = t2.{str(col)} """)
        on_stmt = f" AND ".join(on_clause)


        for col in column_list:
            if str(col).lower().strip() not in ignore_cols:
                col_clause += f"""{str(col).upper()},""" 

        for col in column_list:
            if str(col).lower().strip() not in ignore_cols:
                val_clause += f"""t2.{str(col).upper()},""" 


        statement = self.build_insert_update_merge(tablename, target_table, on_stmt, select_cols, \
                                update_clause, col_clause, val_clause)

        
        return statement

    def build_insert_update_merge(self, tablename, target_table, on_clause, select_cols, \
                                update_clause, col_clause, val_clause): 
        mrg_stmt = f""" MERGE INTO "{database}"."{target_schema}".{target_table}  t1 USING 
           (SELECT DISTINCT {select_cols[:-1]}"""
        mrg_stmt += f""" FROM "{database}"."{target_schema}".{tablename} """  
        mrg_stmt += f""" WHERE CDC_LOAD_TIME >=  DATEADD({period}, {offset}, CURRENT_TIMESTAMP) ) t2 """ 
        mrg_stmt += f""" ON {on_clause} 
                         WHEN MATCHED THEN UPDATE  SET {update_clause[:-1]}"""   
        mrg_stmt += f""" WHEN NOT MATCHED THEN INSERT ({col_clause[:-1]}) """  
        mrg_stmt += f"""                       VALUES ({val_clause[:-1]}); """ 

        return mrg_stmt                            


    def build_table_list(self):
        """ Build a list of transactions to be processed"""
        stmt = f"""
        SELECT TABLENAME, TARGET_TABLE, TABLE_COLUMNS, TARGET_COLUMNS, TABLE_KEY_COLUMN
        FROM "{database}"."{source_schema}"."CDC_TABLE_META_DATA"
        WHERE TABLENAME LIKE 'V_%'
        AND PROCESS_FLAG = TRUE
        """             
        self.SF['cur'].execute(stmt)
        rows = self.SF['cur'].fetchall()
        return rows


if __name__ == '__main__':
    """ This code is provided to run theses funtions locally"""
    CDCSTL = CDCLoadToTransform(SF_CONNECTION)
    main_start = perf_counter()
    run_epoch = time.time()
    print(f"Main Start: {main_start} Offset |{offset}| Period {period}")
    table_list = CDCSTL.build_table_list()
    for table_name, target_table, table_columns, target_columns, key_column in table_list:
        if display:
            print(f"***** {table_name} *****")
        CDCSTL.build_execute_merge(table_name, target_table, table_columns, key_column, target_columns)
    main_end = perf_counter()
    tot_time = main_end - main_start
    print(f"""Main End: {main_start} 
            Total Time: {round((tot_time / 60),1)} minutes 
            """)
    if display:
        print(CDCSTL.WIERD_TABLES)

