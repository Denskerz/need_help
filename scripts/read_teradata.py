import gin
import pathlib as pl
from absl import app, flags
from datetime import datetime

from bicc_ml_pipeline.teradata.teradata_utils import download_data, check_dates,download_data_to_csv

FLAGS = flags.FLAGS
flags.DEFINE_string('config_path', '/ml_data/dag_id', 'path to folder with configs', short_name='p')
flags.DEFINE_string('input_file_path', '/ml_data/027_rtl_deposits/in/feature.csv', 'path to input.csv', short_name='o')
flags.DEFINE_string('input_file_path2', '/ml_data/027_rtl_deposits/in/feature_spr.csv', 'path to input.csv', short_name='o2')


@gin.configurable
def main(
        conn_args, dag_id, schema,
        feat_table,feat_file,
        rep_dt_column_name, max_date_dif_days):
    print("\nStart:", datetime.now())
    print("Check dates")
    tbl1 = f'{schema}.{feat_table[0]}'
    check_dates(conn_args, tbl1, rep_dt_column_name, max_date_dif_days)
    input_file_path = FLAGS.input_file_path

    print("Download features 1 ...")
    df = download_data_to_csv(conn_args,input_file_path, tbl1, sep=";")
    
    tbl2 = f'{schema}.{feat_table[1]}'
    #check_dates(conn_args, tbl2, rep_dt_column_name, max_date_dif_days)
    input_file_path2 = FLAGS.input_file_path2

    print("Download features 2 ...")
    #df = download_data_to_csv(conn_args,input_file_path2, tbl2, sep=";")
    df = download_data(conn_args, tbl2)
    df.to_csv(input_file_path2, index=False, sep=";", encoding='cp1251')




def main_with_gin_config(arg):
    gin.add_config_file_search_path(FLAGS.config_path)
    gin.parse_config_file('config_teradata.gin')
    main()
    return


if __name__ == "__main__":
    app.run(main_with_gin_config)
