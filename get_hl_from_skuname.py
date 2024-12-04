import pandas as pd
import re

def weird_sku_name(x):
    x = re.sub(r'\s|[\d]%','',x)
    a = re.search(r'.+[度|\u4E00-\u9Fa5 ]([0-9.]+ML|[0-9.]+L)([0-9]+[X|*][0-9]+|桶|回桶).+', x)
    if a:
        ml_str = a.group(1)
        bottle_cnt_str = a.group(2)
        liter_int = float(re.match(r'[\d.]+', ml_str).group())/1000 if 'M' in ml_str else float(re.match(r'[\d.]+', ml_str).group())
        bottle_cnt_str = bottle_cnt_str.replace("*", 'X')
        print(bottle_cnt_str)
        if re.search(r'([0-9]+X[0-9]+)', bottle_cnt_str):
            bottle_cnt_int = int(bottle_cnt_str.split('X')[0])*int(bottle_cnt_str.split('X')[1])
        else:
            bottle_cnt_int = 1 if '桶' in bottle_cnt_str else re.search(r'[\d]+', bottle_cnt_str).group()
#         print(x, liter_int, bottle_cnt_int)
        return ml_str, bottle_cnt_str, liter_int, bottle_cnt_int
    else:
        raise ValueError(f"wrong sku name rules {x}")


def get_sku_hl_mapping_table(sku_name_list):
    ml_list = []
    bottle_cnt_list = []
    liter_of_bottle = []
    hl_list = []
    for x in sku_name_list:
        ml_of_bottle = re.search(r'.+[度|\u4E00-\u9Fa5]([0-9]+ML|[0-9]+L)([0-9]+X[0-9]+).+', x)   # 通用规则
        if ml_of_bottle:
            ml_str = ml_of_bottle.group(1)
            bottle_cnt_str = ml_of_bottle.group(2)
            liter_int = float(re.match(r'[\d.]+', ml_str).group())/1000 if 'M' in ml_str else float(re.match(r'[\d.]+', ml_str).group())
            bottle_cnt_int = int(bottle_cnt_str.split('X')[0])*int(bottle_cnt_str.split('X')[1])
            hl = liter_int * bottle_cnt_int/100
        else:
            try: # 特殊规则
                ml_str, bottle_cnt_str, liter_int, bottle_cnt_int = weird_sku_name(x)
                hl = liter_int * bottle_cnt_int / 100
            except:
                print(x)
                ml_str,bottle_cnt_str,liter_int, hl = None
        ml_list.append(ml_str)
        bottle_cnt_list.append(bottle_cnt_str)
        liter_of_bottle.append(liter_int)
        hl_list.append(hl) #换算成百升
    table = pd.DataFrame({
                    'sku_name':sku_name_list,
                    'ml_name':ml_list,
                    'liter_per_bottle': liter_of_bottle,
                    'bottle_cnt_name': bottle_cnt_list,
                    'hl':hl_list
                    })
    return table