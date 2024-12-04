import pandas as pd

# wccs_productid_sap_sku_mapping = spark.sql(
#   """
#   select distinct product_id, cast(sap_skudm as int) as SKUCode from wccs_ods.bas_material
#   """)
# used_sap_sku = spark.sql(
#   """
# SELECT DISTINCT SKUCode, UPPER(Brand_Family_ComDB),ISP_SKU
# FROM bcc_dmdsrintegrationproject_ods.report_import_mapping_sku
# WHERE UPPER(DataSource)='SAP' AND (CASE WHEN UPPER(Brand_Family_ComDB) in ('BUD','HBI','HBO','HKOW') THEN 1 WHEN UPPER(coalesce(ISP_SKU, '')) NOT IN ('NA','','NULL') THEN 1 ELSE 0 END) = 1
#   """
# )
# used_sap_sku = used_sap_sku.join(wccs_productid_sap_sku_mapping, on='SKUCode', how='left')

# def get_sku_hl_mapping():
#     sku_hl_mapping = pd.read_excel('src_data/进销存数据.xlsx', sheet_name='物料信息')
#     sku_hl_mapping['hl'] = sku_hl_mapping ['库存单位净升'] / 100
#     return sku_hl_mapping

def get_used_brand_family():
    sku_brand_family_mapping = pd.read_excel("src_data/SAP_sku_CIO_BRANDFAMILY_Mapping_final.xlsx") # src: Vivian
    # select distinct product_id, cast(sap_skudm as int) as SKUCode from wccs_ods.bas_material
    sku_hl_mapping = pd.read_excel('src_data/skucode_hl_mapping.xlsx')   # from Charlotte code
    product_id_mapping = pd.read_csv("src_data/sap_code_wccs_product_id_mapping.csv") # wccs_ods.bas_material # select distinct product_id, cast(sap_skudm as int) as SKUCode from wccs_ods.bas_material
    used_brand_family = ['ISP', 'BUD', 'HBI', 'HBO', 'HKOW']
    used_brand_family_mapping = sku_brand_family_mapping[sku_brand_family_mapping['Brand Family'].isin(used_brand_family)].drop_duplicates(subset=['SKUCode'])[['SKUCode','Brand Family']]    
    # brand family data 匹配product id和百升数
    product_id_mapping = pd.merge(product_id_mapping, sku_hl_mapping, on='SKUCode', how='left')   # Pcode, SKUCode,HL
    used_brand_family_mapping = pd.merge(used_brand_family_mapping, product_id_mapping, on='SKUCode', how='left')
    used_brand_family_mapping['product_id'] = used_brand_family_mapping['product_id'].str.upper()
    used_wccs_sku_list = list(used_brand_family_mapping['product_id'].unique())
    return used_wccs_sku_list, used_brand_family_mapping, product_id_mapping

# def get_used_brand_family():
#     """
#         used_wccs_sku_list 四个brandfamily或isp_sku不为空的wccs_product_id list
#         sku_filtered: product id mapping表 SKUCode，upper(Brand_Family_ComDB)，ISP_SKU，product_id
#     """
#     # 期初库存数据的SKU需要删选
#     # sku_mapping = pd.read_excel(r"src_data/sku mapping.xlsx")
#     # used_brand_family = ['BUD', 'HBI', 'HBO', 'HKOW']
#     # sku_filtered = sku_mapping[sku_mapping['Brand Family ComDB'].isin(used_brand_family) | sku_mapping['ISP_SKU'].notnull()]
#     sku_filtered = pd.read_excel(r"src_data/used_sku_product_id.xlsx")
#     used_wccs_sku_list = sku_filtered['product_id'].dropna().drop_duplicates().tolist()
#     return used_wccs_sku_list, sku_filtered

def get_inv_data(used_wccs_sku_list, sku_filtered, used_qty_col='期初库存'):
    """
        箱数_col：期初库存
        hl_col: inv_hl     
    """
    # # begin_inv = pd.read_csv('src_data/库存余额表DB-202212.csv')       # 2022年末的期末库存就是2023年的期初库存
    begin_inv = pd.read_excel('src_data/进销存数据.xlsx')
    begin_inv['物料代码'] = begin_inv['物料代码'].str.upper()
    begin_inv = begin_inv[begin_inv['物料代码'].isin(used_wccs_sku_list)]
    sku_filtered_info = sku_filtered.sort_values(by='product_id').drop_duplicates(subset=['product_id'])
    sku_filtered_info = sku_filtered_info[sku_filtered_info['product_id'].notnull()]
    begin_inv = pd.merge(begin_inv, sku_filtered_info[['product_id', 'SKUCode',  'HL']], left_on='物料代码',right_on='product_id', how='left')
    unfound_sku = begin_inv[begin_inv['SKUCode'].isnull()]
    if len(unfound_sku) > 0:
        print("没对应上SKUCode/HL的wccs product id", unfound_sku['product_id'].unique())
    begin_inv['inv_hl'] = begin_inv[used_qty_col] * begin_inv['HL']    # 转换成百升
    return begin_inv

def get_stw_data(start=202101, end=202410):
    """
        箱数_col：stw_qty
        hl_col: stw_hl
        month_col: billingyearmonth
    """
    # # stw = pd.read_excel('src_data/stw_str_241118.xlsx', sheet_name='STW')        # 2023.1.1~2023.12.31
    stw = pd.read_excel('src_data/STW_STR_20241129.xlsx', sheet_name='STW')          # 2021.1~2024.10
    stw.columns = [x.lower() for x in stw.columns]
    stw = stw[(stw['billingyearmonth'] >= start) & (stw['billingyearmonth'] <= end)]
    return stw

def get_str_data(sku_filtered, start=202101, end=202410):
    """
        箱数_col：str_qty
        hl_col: str_hl
        month_col: month
    """
    # # ws_str = pd.read_excel('src_data/stw_str_241118.xlsx', sheet_name='STR')
    ws_str = pd.read_excel('src_data/STW_STR_20241129.xlsx', sheet_name='STR')       # 2021.1~2024.11
    ws_str = ws_str[(ws_str['month'] >= start) & (ws_str['month'] <= end)]
    sku_filtered_info = sku_filtered.sort_values(by='SKUCode').drop_duplicates(subset=['SKUCode'])
    ws_str = pd.merge(ws_str, sku_filtered_info[['SKUCode', 'HL']], left_on='skucode', right_on='SKUCode', how='left')
    unfound_sku = ws_str[ws_str['HL'].isnull()]
    if len(unfound_sku) > 0:
        print(unfound_sku)
    ws_str['str_hl'] = ws_str['str_qty'] * ws_str['HL']
    return ws_str