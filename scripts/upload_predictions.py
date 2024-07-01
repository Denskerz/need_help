# -*- coding: cp1251 -*-
import gin
from pathlib import Path
from absl import app, flags
import pandas as pd
from bicc_ml_pipeline.teradata.teradata_utils import upload_binary_prediction, stage_pred_to_dm,write_to_db_table,update_pred_header,drop_pred_from_stage
from fastload import fastload_write_to_db_table
from loguru import logger
FLAGS = flags.FLAGS
flags.DEFINE_string('config_path', '/ml_data/dag_id', 'path to folder with configs', short_name='p')


@gin.configurable
def main(
        dag_id, model_id, pred_file_name, feat_file_name,
        conn_args, stage_schema, pred_tbl_name, dm_schema,
        error_schema,
):
    folder_path = f"/ml_data/{dag_id}"
    feat_file_fpath = f"{folder_path}/in/{feat_file_name[0]}"
    pred_fpath = f"{folder_path}/out/{pred_file_name}"
    pred_df = pd.read_csv(pred_fpath, sep=',').drop_duplicates()
    col_name_map = {}
    
    pred_encoding = None#'cp1251'
    ext_header = None
    
    
    pred_header_ids = update_pred_header(
        feat_file_fpath,
        conn_args,
        dm_schema,
        model_id,
        ext_header, pred_encoding)
    #drop_pred_from_stage(pred_header_ids, conn_args, dm_schema)
    pred_df.insert(0,['PRED_HEADER_ID'],pred_header_ids[model_id],True)
    pred_df.insert(1,['MODEL_ID'],model_id,True)
    logger.info(f'started writing to db via fastload')
    fastload_write_to_db_table(df=pred_df, schema=stage_schema, error_schema=error_schema, table_name=pred_tbl_name,conn_args=conn_args)
    #fastload_write_to_db_table(df=pred_df, schema=stage_schema, table_name=pred_tbl_name)
    logger.info(f'ended writing to db via fastload')
    #write_to_db_table(df=pred_df, schema=stage_schema, table_name=pred_tbl_name)
    #stage_pred_to_dm(pred_header_ids, conn_args, dm_schema)

    return


def main_with_gin_config(arg):
    gin.add_config_file_search_path(FLAGS.config_path)
    gin.parse_config_file('config_upload_predictions.gin')
    main()
    return


if __name__ == "__main__":
    app.run(main_with_gin_config)
    print("\nPredictions uploaded")
