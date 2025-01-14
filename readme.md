数据：
期初库存/期末库存 均为手工数据，期初库存的经销商代码为300开头，产品代码是P code
经销商W码和300码转换关系 bcc_baseline_ods.wccs_ws
skucode 和 Pcode 对应关系：wccs_ods.bas_material   
目前期初库存数据表用的是期初库存字段（给的数据是2021年1月的）
期末库存数据用的是期末库存，给的是202412月的

问题：会有配不上百升数的SKU，也会有Pcode-sap code对应不上导致有些sku的期初库存就被忽略的问题

## 代码文件：
### fetch_data

主要功能是取数
core functions:
1. skucode 对应的百升数 get_sku_hl_mapping_table
2. skucode 和 Pcode 对应关系 get_sku_sap_wccs_code_mapping_table
3. 经销商的名字和对应关系：budtech_brewdat_prod_ods.abi_cloud_wholesaler_ws_wholesaler
4. 导入期初库存数据并且转换sku的百升数，filter需要的brand get_inv_data
5. get_stw_data(start=202101, end=202410) 获取原始stw数据，stw表自带百升数
6. get_str_data 获取原始str数据，配上sku百升数
7. get_all_t1_t15_comb 从t1卖给t1.5的str数据中获取挂在t1下面的t1.5的组合
select payercode as t15_code, sj_pay_to as t1_code  
        from finance_ds_inventory_dmt.finance_poc_datahub_mid_ws_wccs_str 
        where deleted = 0  
        group by all
8. get_t15_stw_monthly_from_t1  计算t1.5从t1每个月的进货，by brand by t1.5ws
9. get_t15_str_monthly  挂在t1下面的t1.5每月卖给售点的str的量

### fifo_all_ws

get_stw_monthly_balance 核心算法，模拟期初库存和第i个月的进货在第N个月的库存剩余，或者write off量（过期库存）
主要参数：
`data_type`: t1 或者t15均表示计算从ABI进货的库存情况, t1_t15 为模拟计算从t1进货的t1.5的库存情况（最后要算回到每个t1下面，即汇总t1下面的所有t1.5的过期库存）
`agg_col`: 默认是用箱数qty计算，这里的定义是为了转百升数，用百升数算库存
ws_params_dict_inv['agg_col'] = 'inv_hl'
ws_params_dict['agg_col'] = 'stw_hl'
ws_params_dict_str['agg_col'] = 'str_hl'
设置过期月份数，可以每个brand一个过期月份数
如果6个月过期，意味着1月进来的货，7月还可以用最后一次，那么7月的剩余已经可以算作过期库存了
inv_expire_month = 12    # 库存9个月后失效，之前是6个月
brand_expire_month_dict = {'BUD': inv_expire_month, 'HBI': inv_expire_month, 'HBO': inv_expire_month, 'HKOW':inv_expire_month, 'ISP': inv_expire_month}
步骤：
1. 获取期初库存，stw, str的by ws by brand monthly数据
2. 剔除ws
3. 区分有str的经销商合没str的经销商，
4. 分别模拟库存，各自落表
table_name = f"finance_ds_inventory_dmt.inventory_details_{inv_expire_month}_{data_type}_{data_version}"
parquet_path = f'/mnt/srf/inv/fifo_inventory_details_month{inv_expire_month}_{data_type}_{data_version}'
对于no_str的表，在最后加上_no_str即可

### ws_analysis

计算by ws by brand的过期库存汇总数据

根据明细表算by brand by ws的汇总，需要指定截止月是哪个月，end_month = -1为数据最后一个月
12个月的有效期，2024年12月计算过期库存时，计算从期初库存到2023年12月的write off 库存加总

expire_month = 12   # 库存12个月后过期
data_type = 't1'    # or t15
end_month = -1   # 数据的最后一个月，202412   -3是202410

跑T1的结果需要用到两张明细表，一张是T1全量的，一张是挂在t1下面的t1.5的
if data_type == "t1":
    # data_version加"_no_str"是没有str的经销商的结果
    data_version = 'v2025010302'        # t1读数版本
    t1_t15_data_version = 'v20250107'   # t1.5读数版本
`get_one_month_result` 用于计算截止到某个月的汇总数据，如果要计算多个月需要反复跑
`get_t15_wf_items` 用于将t1下面的t1.5的剩余库存汇总起来，生成两列，一个是汇总的过期库存，一个是t1.5的json明细

cell t1 summary: 从ABI进货的T1的库存模拟统计
cell t1_t15 summary 为计算挂在t1下面的t15的明细汇总表，by ws by brand
cell t15全量： 算从ABI进货的t1.5的库存模拟的overview
t1 落表：
f"finance_ds_inventory_dmt.fifo_inv_overview_{expire_month}_{data_version}"
从t1进货的t1.5的落表
f"finance_ds_inventory_dmt.fifo_inv_overview_t1_t15_{expire_month}_{t1_t15_data_version}"
从abi进货的t1.5的落表
 f"finance_ds_inventory_dmt.fifo_inv_overview_{expire_month}_{data_version}_t15_full"
