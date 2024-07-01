import os
import teradatasql
from loguru import logger
import gin

@gin.configurable
def fastload_write_to_db_table(
        df,            #
        schema,        # gin
        error_schema,  # gin
        table_name,    # gin
        conn_args=None,# gin
):
    assert not df is None
    host=conn_args.get("host")          # gin
    user=conn_args.get("user")          # gin
    password=conn_args.get("password")  # gin
    upload_filepath = 'temp_predictions.csv'
    # создаем временный файл, который содержит все необходимые для записи атрибуты
    df.to_csv(upload_filepath, index=False)
    print('1')
    with teradatasql.connect(host=host, user=user, password=password, manage_error_tables=False) as con: #
        print('2')
        with con.cursor () as cur:
            print('3')
            sTableName = f"{schema}.{table_name}"
            eTableName = f"{error_schema}.{table_name}"
            try:
                print("con.autocommit = False")
                con.autocommit = False
                try:
                    # 6 колонок в файле, который загружаем
                    sInsert = "{fn teradata_manage_error_tables_off}{fn teradata_require_fastload}{fn teradata_error_table_database(SBX_RTL)}{fn teradata_sessions(1)}{fn teradata_read_csv(" + upload_filepath + ")} INSERT INTO " + sTableName + " (?, ?, ?, ?, ?, ?)"
                    cur.execute(sInsert)

                    # obtain the warnings and errors for transmitting the data to the database -- the acquisition phase
                    sRequest = "{fn teradata_nativesql}{fn teradata_get_warnings}" + sInsert
                    logger.info(sRequest)
                    cur.execute(sRequest)
                    [logger.warning(row) for row in cur.fetchall()]
                    sRequest = "{fn teradata_nativesql}{fn teradata_get_errors}" + sInsert
                    logger.info(sRequest)
                    cur.execute(sRequest)
                    [logger.error(row) for row in cur.fetchall()]

                    # docs: specifies whether the database should allocate a LSN for this session. 0 = no LSN
                    sRequest = "{fn teradata_nativesql}{fn teradata_logon_sequence_number}" + sInsert
                    logger.info(sRequest)
                    cur.execute(sRequest)
                    [logger.info(row) for row in cur.fetchall()]

                    con.commit()

                    # obtain the warnings and errors for the apply phase
                    sRequest = "{fn teradata_nativesql}{fn teradata_get_warnings}" + sInsert
                    logger.info(sRequest)
                    cur.execute(sRequest)
                    [logger.warning(row) for row in cur.fetchall()]
                    sRequest = "{fn teradata_nativesql}{fn teradata_get_errors}" + sInsert
                    logger.info(sRequest)
                    cur.execute(sRequest)
                    [logger.error(row) for row in cur.fetchall()]

                finally:
                    con.autocommit = True

            finally:
                pass
    os.remove(upload_filepath)
