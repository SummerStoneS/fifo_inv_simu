import pandas as pd
import re
from datetime import datetime
import warnings
import yaml
warnings.filterwarnings('ignore')
from fetch_data import *
from get_hl_from_skuname import *


def make_monthly_horizontal_df(lst, name):
    df = pd.DataFrame(lst, columns=[name]).T
    df.columns = [x + 1 for x in df.columns]
    df = df.add_prefix('month_')
    return df


def get_initial_inv(data, **params):
    ws_payer_code = params['ws_payer_code']
    ws_id_name = params['ws_id_name']
    ws_inv = data.query(f"{ws_id_name} == '{ws_payer_code}'")
    if params['agg_col'] == 'hl':
        ws_inv[params['sku_name']] = ws_inv[params['sku_name']].str.upper()
        ws_inv[params['sku_name']] = ws_inv[params['sku_name']].apply(lambda x: re.sub(r'\s|[\d]%', '', x))
        sku_name_list = list(ws_inv[params['sku_name']].unique())
        sku_hl_mapping_table = get_sku_hl_mapping_table(sku_name_list)
        ws_inv = pd.merge(ws_inv, sku_hl_mapping_table[['sku_name', 'hl']], left_on=params['sku_name'], right_on='sku_name')
    else:
        sku_hl_mapping_table = pd.DataFrame()
    ws_inv_agg = ws_inv.groupby([ws_id_name])[params['agg_col']].sum()
    return ws_inv_agg, sku_hl_mapping_table


def get_num_list(stw, **params):
    # STW/STR 12个月的值
    ws_stw = stw.query(f"{params['ws_id_name']} == {params['ws_payer_code']}")
    if params['agg_col'] == 'hl':   # ws stw表里的agg_col是stw_hl
        ws_stw[params['sku_name']] = ws_stw[params['sku_name']].str.upper()
        ws_stw[params['sku_name']] = ws_stw[params['sku_name']].apply(lambda x: re.sub(r'\s|[\d]%', '', x))
        sku_name_list = list(ws_stw[params['sku_name']].unique())
        sku_hl_mapping_table = get_sku_hl_mapping_table(sku_name_list)
        ws_stw = pd.merge(ws_stw, sku_hl_mapping_table[['sku_name', 'hl']], left_on=params['sku_name'], right_on='sku_name')
    else:
        sku_hl_mapping_table = pd.DataFrame()
    ws_stw_agg = ws_stw.groupby([params['month_col']])[params['agg_col']].sum()
    ws_stw_agg = ws_stw_agg.reindex(stw_yr_month_index).fillna(0)
    ws_stw_lst = ws_stw_agg.to_list()
    return ws_stw_lst, sku_hl_mapping_table


def get_stw_monthly_balance(begin_inv, stw: list, out_str: list, inv_expire_month=6, simu_months=12):
    """
    begin_inv:202212期末库存=2023年1月期初库存
    stw: 2023年12个月的stw进货量
    out_str:2023年12个月的出货量
    simu_months: 模拟的月份数，默认是1年即12个月
    result: 期初库存每个月的结余
    """
#     # 测试数据
#     begin_inv = 80
#     stw = [80]*2 + [100]*10     # 进货
#     out_str = [80]*7 + [80]*5

    begin_inv_remain = begin_inv
    stw_remain = stw.copy()             #  截止到当前月每个月的进货量还剩多少
    stw_remain_monthly = []             # 每个月的结果合集
    begin_inv_remain_monthly = []       # 每个月的结果合集
    #     begin_inv_remain_lst = [None] * 12
    #     stw_remain_list = []        # 每个月进货stw的剩余
    str_remain_list = []
    for i in range(simu_months):
        stw_remain_list = []
        if (out_str[i] <= begin_inv_remain) and (i <= (inv_expire_month - 1)):     # 期初库存能扣到6月
            begin_inv_remain -= out_str[i]  # 期初（年初）库存的剩余
            stw_remain_list = stw_remain.copy()     # 每个月stw的剩余
            stw_remain_list = [stw_remain_list[stw_month] if stw_month <= i else None for stw_month in range(simu_months) ]
            str_remain = 0
            str_remain_list.append(str_remain)
        else:
            if (begin_inv_remain > 0) and (i <= (inv_expire_month - 1)):
                str_remain = out_str[i] - begin_inv_remain     # 这个月还欠多少货
                begin_inv_remain = 0
            else:
                str_remain = out_str[i]
            for j in range(len(stw_remain)):
                if i > (inv_expire_month + j):
                    pass                             # 如果到了8月，1月进货的stw已经不能用了
                else:
                    delta = min(str_remain, stw_remain[j])   # 这个月的剩余需求和当前月的进货余量
                    str_remain -= delta
                    stw_remain[j] -= delta
                if j <= i:
                    stw_remain_list.append(stw_remain[j])    #  大于当前月的进货不能用
                else:
                    stw_remain_list.append(None)
                if j == i:
                    str_remain_list.append(str_remain)       # 用于检验这个月str需求是否全被满足了
        stw_remain_monthly.append(stw_remain_list)
        begin_inv_remain_monthly.append(begin_inv_remain)
    #     return stw_remain_monthly, begin_inv_remain_monthly, str_remain_list
    stw_replenish_df = make_monthly_horizontal_df(stw, 'STW(+)')
    str_remain_df = make_monthly_horizontal_df(str_remain_list, 'str_remain')
    out_str_df = make_monthly_horizontal_df(out_str, 'STR(-)')
    begin_inv_remain_df = make_monthly_horizontal_df(begin_inv_remain_monthly, 'openning_balance')
    stw_balance_df = pd.DataFrame(stw_remain_monthly, index=range(1, simu_months + 1), columns=range(1, simu_months + 1)).T
    stw_balance_df = stw_balance_df.add_prefix('month_')
    stw_balance_df.index = stw_balance_df.index.map(lambda x: str(x) + '_STW_balance')
    output = pd.concat([stw_replenish_df, out_str_df, begin_inv_remain_df, stw_balance_df, str_remain_df])
    year_begin_inv_balance = pd.DataFrame([begin_inv] + [None]*(len(output)-1), index=output.index, columns=['Openning Balance'])
    return pd.concat([year_begin_inv_balance, output], axis=1)


def generate_fifo_inventory_balance(used_ws_dict, begin_inv, stw, ws_str, col='stw_qty', inv_expire_month=6, simu_months=12):
    """
        used_ws_dict: 要模拟库存的经销商
        begin_inv:年初库存
        stw: 所有经销商的stw数据
        ws_str: 所有经销商的str出货数据
        col: stw_qty箱数 or hl百升数; 算库存箱数还是百升数
        inv_expire_month: 多少个月后酒库存的就过期了
        simu_months: 模拟的月份数，默认是1年即12个月
    """
    dt_string = datetime.now().date().strftime("%m%d")
    file = pd.ExcelWriter(f'result_data_{col}_{dt_string}_{inv_expire_month}.xlsx')
    sku_hl_mapping_df = pd.DataFrame()
    result_df_dict = {}
    for ws_code, payer_code in used_ws_dict.items():
        print(f"ws_code:payer_code {ws_code}:{payer_code}")
        #     ws_stw_lst = ws_stw_agg.to_list()
        ws_params_dict_inv['ws_payer_code'] = ws_code
        ws_params_dict['ws_payer_code'] = payer_code
        ws_params_dict_str['ws_payer_code'] = payer_code
        if col == 'hl':                                      # 通过洗sku的名字算hl
            ws_params_dict_inv['agg_col'] = 'hl'
            ws_params_dict['agg_col'] = 'stw_hl'
            ws_params_dict_str['agg_col'] = 'hl'
        elif col == 'hl_src':                                # 取数算的百升
            ws_params_dict_inv['agg_col'] = 'inv_hl'
            ws_params_dict['agg_col'] = 'stw_hl'
            ws_params_dict_str['agg_col'] = 'str_hl'
        else:                                                # qty箱数
            pass
        ws_begin_inv,sku_hl_mapping_inv = get_initial_inv(begin_inv, **ws_params_dict_inv)   # 获取年期初库存
        ws_stw_lst, _ = get_num_list(stw, **ws_params_dict)                  # 获取每个月的进货量stw
        ws_str_lst,sku_hl_mapping_str = get_num_list(ws_str, **ws_params_dict_str)           # 获取每个月的出货量str
#         if min(len(ws_stw_lst), len(ws_str_lst)) < simu_months:
#             continue
        ws_begin_inv = 0 if len(ws_begin_inv) == 0 else ws_begin_inv.values[0]
        result_df = get_stw_monthly_balance(begin_inv=ws_begin_inv, stw=ws_stw_lst, out_str=ws_str_lst, inv_expire_month=inv_expire_month, simu_months=simu_months)
        result_df.to_excel(file, sheet_name=ws_code)
        result_df_dict[ws_code] = result_df
        if len(sku_hl_mapping_inv) > 0:
            sku_hl_mapping_inv['type'] = 'inv'
            sku_hl_mapping_inv['ws_code'] = ws_code
            sku_hl_mapping_df = pd.concat([sku_hl_mapping_df, sku_hl_mapping_inv])
        if len(sku_hl_mapping_str) > 0:
            sku_hl_mapping_str['type'] = 'str'
            sku_hl_mapping_str['ws_code'] = ws_code
            sku_hl_mapping_df = pd.concat([sku_hl_mapping_df, sku_hl_mapping_str])
    if len(sku_hl_mapping_df) > 0:
        sku_hl_mapping_df.to_excel(file, sheet_name='sku_hl_mapping')
    file.close()
    return result_df_dict


if __name__ == '__main__':
    used_ws_dict = {'W00013686': 30018687, 'W00014676': 30019378, 'W00008121': 30012653}
    with open('config.yml', 'r', encoding='utf-8') as file:
        params_dict = yaml.safe_load(file)
    ws_params_dict_inv = params_dict['ws_params_dict_inv']  # inv数据使用列名
    ws_params_dict = params_dict['ws_params_dict']     # stw数据 列名
    ws_params_dict_str = params_dict['ws_params_dict_str']   # str列名

    # 获取原始数据
    used_wccs_sku_list, sku_filtered, product_id_mapping = get_used_brand_family()  # sku_filtered是过滤了四个brand的sku mapping表；product_id_mapping是全量的sku hl mapping表
    begin_inv = get_inv_data(used_wccs_sku_list, sku_filtered, used_qty_col='期初库存')
    stw = get_stw_data(start=202101, end=202410)
    ws_str = get_str_data(product_id_mapping, start=202101, end=202410)

    # 检查数据的月份数是不是完整，做结果表的index
    month_cnt = len(stw['billingyearmonth'].unique())  # 2021.1到24年10月是46个月；每个经销商都要46个月
    print(f"stw月份数：{month_cnt}")
    print(f"str月份数：{len(ws_str['month'].unique())}")
    stw_yr_month_index = stw['billingyearmonth'].drop_duplicates().sort_values()

    inv_unit = 'hl_src'  # stw_qty 或其他值，都是指默认值，箱数/////hl_src是原始数据带的百升数/////hl是根据sku洗出来的百升数
    inv_expire_month = 9  # 库存9个月后失效，之前是6个月
    result_df_dict = generate_fifo_inventory_balance(used_ws_dict, begin_inv, stw, ws_str, col='hl_src',
                                                     inv_expire_month=inv_expire_month, simu_months=month_cnt)
