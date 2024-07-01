import pandas as pd
import pickle
from loguru import logger
from config import input_filepath, csv_sep, csv_encoding, csv_decimal
from config import main_mdl_cols_filepath, main_mdl_filepath
from config import agmt_cury_cd_mdl_filepath, agmt_cury_cd_target_mapper_filepath, agmt_cury_cd_cols_filepath
from config import is_online_mdl_filepath, is_online_cols_filepath
from config import is_otz_mdl_filepath, is_otz_target_mapper_filepath, is_otz_cols_filepath
from config import offer_sum_loc_mdl_filepath, offer_sum_loc_cols_filepath
from config import agmt_rate_mdl_filepath, agmt_rate_cols_filepath, agmt_rate_target_mapper_filepath
from config import term_planned_mdl_filepath, term_planned_cols_filepath, term_planned_target_mapper_filepath
from config import output_filepath
from datetime import datetime

def make_prediction(
        data,
        model_filepath=None,
        cols_filepath=None,
        target_mapper_filepath=None,
        series_or_dataframe=None,
        target_col=None,
        method='predict_proba',
) :

    if cols_filepath:
        with open(cols_filepath, 'rb') as f:
            x_cols = pickle.load(f)

    if model_filepath:
        with open(model_filepath, 'rb') as f:
            mdl = pickle.load(f)

    if target_mapper_filepath:
        with open(target_mapper_filepath, 'rb') as f:
            target_mapper = pickle.load(f)

    if series_or_dataframe == 'series':
        if method == 'predict_proba' and 'automl' not in model_filepath:
            preds = pd.Series(data=mdl.predict_proba(data[x_cols])[:, 1], index=data.index, name=target_col)
        elif method == 'predict_proba' and 'automl' in model_filepath:
            preds = pd.Series(data=mdl.predict_proba(data.reset_index()[x_cols+['REP_DT','PRTY_ID']])[:, 1], index=data.index, name=target_col)
        elif method == 'predict':
            preds = pd.Series(data=mdl.predict(data[x_cols]), index=data.index, name=target_col)
    elif series_or_dataframe == 'dataframe':
        assert method == 'predict_proba'
        if target_col in ('agmt_cury_cd', 'agmt_rate', 'term_planned'):
            preds = pd.DataFrame(
                data=mdl.predict_proba(data.reset_index()[x_cols+['REP_DT','PRTY_ID']])[:, 4:],
                index=data.index,
                columns=[f'{target_col.lower()}_{str(x)}' for x in target_mapper.keys()]
            )
        elif target_col == 'is_otz':
            preds = pd.DataFrame(
                data=mdl.predict_proba(data[x_cols])[:, :],
                index=data.index,
                columns=[f'{target_col.lower()}_{str(x)}' for x in target_mapper.keys()]
            )

    return preds


if __name__ == '__main__':
    tera = pd.read_csv(input_filepath, sep=csv_sep, encoding=csv_encoding, decimal=csv_decimal)
    tera = tera.sort_values(['PRTY_ID']).set_index(['PRTY_ID', 'REP_DT'], )
    tera.rename(columns={
        'signed_last_90'.upper(): 'signed_last_90',
        'expires_in_30'.upper(): 'expires_in_30',
        'days_since_last_opened_active'.upper(): 'days_since_last_opened_active',
        'days_since_last_opened_inactive'.upper(): 'days_since_last_opened_inactive',
        'share_bezotz_all'.upper(): 'share_beztoz_all',
        'share_beztoz_all'.upper(): 'share_beztoz_all',
        'PFMRD_d_CNT_MIN'.upper(): 'PFMRD_d_CNT_MIN',
        'expires_in_90'.upper(): 'expires_in_90',
        'share_online_all'.upper(): 'share_online_all',
        'share_online_active'.upper(): 'share_online_active',
        'share_otz_all'.upper(): 'share_otz_all',
        'OST_3014_84_SUMMA_OST': 'OPER_3014_84_SUMMA_OST',
    }, inplace=True)

    # main
    main_preds = make_prediction(
        data=tera,
        model_filepath=main_mdl_filepath,
        cols_filepath=main_mdl_cols_filepath,
        series_or_dataframe='series',
        target_col='main',
        method='predict_proba',
    )

    agmt_cury_cd_preds = make_prediction(
        data=tera,
        model_filepath=agmt_cury_cd_mdl_filepath,
        cols_filepath=agmt_cury_cd_cols_filepath,
        target_mapper_filepath=agmt_cury_cd_target_mapper_filepath,
        series_or_dataframe='dataframe',
        target_col='agmt_cury_cd',
        method='predict_proba',
    )

    is_online_preds = make_prediction(
        data=tera,
        model_filepath=is_online_mdl_filepath,
        cols_filepath=is_online_cols_filepath,
        series_or_dataframe='series',
        target_col='is_online',
        method='predict_proba',
    )

    is_otz_preds = make_prediction(
        data=tera,
        model_filepath=is_otz_mdl_filepath,
        cols_filepath=is_otz_cols_filepath,
        target_mapper_filepath=is_otz_target_mapper_filepath,
        series_or_dataframe='dataframe',
        target_col='is_otz',
        method='predict_proba'
    )

    offer_sum_loc_preds = make_prediction(
        data=tera,
        model_filepath=offer_sum_loc_mdl_filepath,
        cols_filepath=offer_sum_loc_cols_filepath,
        series_or_dataframe='series',
        target_col='offer_sum_loc',
        method='predict'
    )

    agmt_rate_preds = make_prediction(
        data=tera,
        model_filepath=agmt_rate_mdl_filepath,
        target_mapper_filepath=agmt_rate_target_mapper_filepath,
        cols_filepath=agmt_rate_cols_filepath,
        series_or_dataframe='dataframe',
        target_col='agmt_rate',
        method='predict_proba'
    )

    term_planned_preds = make_prediction(
        data=tera,
        model_filepath=term_planned_mdl_filepath,
        cols_filepath=term_planned_cols_filepath,
        target_mapper_filepath=term_planned_target_mapper_filepath,
        series_or_dataframe='dataframe',
        target_col='term_planned',
        method='predict_proba'
    )
    df = pd.concat([main_preds, agmt_cury_cd_preds, is_online_preds, is_otz_preds, agmt_rate_preds, term_planned_preds,
                    offer_sum_loc_preds], axis=1)


    output = pd.melt(
        frame=df.reset_index().drop('REP_DT', axis=1),
        id_vars='PRTY_ID',
        value_vars=df.columns,
        var_name='CTGY_1_CD',
        value_name='PRED_VAL'
    )
    inverse_ix_2_name = {
         4000: 'main',
         4001: 'is_online',
         4002: 'offer_sum_loc',
         4003: 'is_otz_-1.0',
         4004: 'is_otz_0.0',
         4005: 'is_otz_1.0',
         4006: 'agmt_cury_cd_62.0',
         4008: 'agmt_cury_cd_84.0',
         4009: 'agmt_cury_cd_108.0',
         4010: 'agmt_cury_cd_113.0',
         4011: 'agmt_rate_(0.01, 3.0]',
         4012: 'agmt_rate_(3.0, 4.5]',
         4013: 'agmt_rate_(4.5, 8.5]',
         4014: 'agmt_rate_(8.5, 17.5]',
         4015: 'term_planned_(35.0, 90.0]',
         4016: 'term_planned_(90.0, 183.0]',
         4017: 'term_planned_(183.0, 366.0]',
         4018: 'term_planned_(366.0, 1150.0]'
    }
    ix_2_name = {v:k for k,v in inverse_ix_2_name.items()}

    now_str = str(datetime.now())[:19]
    output['CREATION_DTTM'] = now_str
    # output.to_pickle('debug.pkl')
    output['CTGY_1_CD'] = output['CTGY_1_CD'].map(ix_2_name)
    print(output.shape)
    print(output['CTGY_1_CD'].value_counts(normalize=True))
    print(output['CTGY_1_CD'].isnull().mean())
    output['CTGY_1_CD'] = output['CTGY_1_CD'].astype(int)
    output.to_csv(output_filepath, index=False)  # .reset_index()['PRTY_ID', 'PRED_VAL', 'CTGY_1_CD', 'CREATION_DTTM']


