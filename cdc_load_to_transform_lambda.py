import json
import time
import cdc_load_to_transform_config as config
from cdc_load_to_transform_connect import snowflake_connector
from snowflake.connector import ProgrammingError
from time import perf_counter
from cdc_load_to_transform_local import CDCLoadToTransform

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

def cdc_load_to_transform(event=None, context=None):
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

