### project description:
根据先进先出原则，by 经销商计算每月进货的STW在每个月出货STR后，还剩多少
- SKU scope: 'ISP', 'BUD', 'HBI', 'HBO', 'HKOW'
  - 数据src是vivian给的`src_data/SAP_sku_CIO_BRANDFAMILY_Mapping_final.xlsx`
- 经销商scope:
  - W00013686 南通荣尚酒业有限公司 0030018687
  - W00014676 广东瑞丰和酒业有限公司潮州仓 0030019378
  - W00008121 深圳市佰兴泰贸易有限公司 0030012653
- 时间范围：
  - 2023.1.1~2023.12.31
- 保鲜期`inv_expire_month` 目前默认是9 
  - 如1月进货，1+inv_expire_month是最后一个能卖的月份
- 计算库存的单位`inv_unit`
  - 箱数：不考虑一箱里面瓶子的规格，只计算箱数
  - 百升数：两种算法，如果`inv_unit` = hl_src则是根据取数算出来的百升数，若是hl，则是根据洗名字判断出百升数
  - 目前箱数和hl_src都算了
- 数据问题
  - Vivian给的sku brandfamily的mapping表的sku code是sap code;库存数据是wccs product_id, 不管是用vivian给的code mapping表还是我们取的， 都无法给SKU brand family表里所有sku配到Product id，意味着经销商库存SKU有些就没有统计到
  - June取的STW和STR包括的brand family可能不是Vivian的版本，会有出入
