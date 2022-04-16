# 최신 종목 코드를 불러와서 상장폐지된 종목을 제외하고
# 종목별로 저장된 테이블을 불러 transpose() '행/렬 변경' 하고
# 모든 종목을 하나의 테이블에 통합하여 새로운 DB(stock_market)에 저장

import pandas as pd
import sqlite3
# import time

# 종목코드 가져오기
code_df = pd.read_html('http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13', header=0)[0]

# 종목코드가 6자리이기 때문에 6자리를 맞춰주기 위해 설정해줌
code_df.종목코드 = code_df.종목코드.map('{:06d}'.format)

# 우리가 필요한 것은 회사명과 종목코드이기 때문에 필요없는 column들은 제외해준다.
code_df = code_df[['회사명', '종목코드']]
# %%

con = sqlite3.connect("c:/Develop/Data_science/stock_market/db/finantial_stmt.db")
table_list = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", con, index_col=None)

# table_list에서 table name에 'C'가 포함되어 있는 table 추출
table_list1 = table_list[table_list['name'].str.contains('C')]

# %% table transpose() 샘플링

test_t = pd.read_sql("SELECT * FROM C155660", con, index_col=None)

test_t = test_t.transpose()

test_t.rename(columns=test_t.iloc[0],inplace=True)
test_t = test_t.drop(test_t.index[0])

test_t.reset_index(drop=False, inplace=True)

test_t.rename(columns={'index':'p_year'}, inplace=True)

test_t['company'] = 'C155660'

# %% 새 DB 컬럼 정의

company_all = pd.DataFrame(columns=list(test_t))

# %%
for code in code_df['종목코드']:
    c_code = 'C' + code
    
    if(table_list['name'].str.contains(c_code)).any():
        sql = "SELECT * FROM " + c_code
        table_tr = pd.read_sql(sql, con, index_col=None)
        table_tr = table_tr.transpose()
        
        table_tr.rename(columns=table_tr.iloc[0],inplace=True)
        table_tr = table_tr.drop(table_tr.index[0])
        table_tr.reset_index(drop=False, inplace=True)
        table_tr.rename(columns={'index':'p_year'}, inplace=True)
        table_tr['company'] = c_code
        
        company_all = company_all.append(table_tr)
        
    else:
        continue

# %% 실적연도(p_year) q2020_3 을 p2020_03 으로 변경

company_all = company_all.replace({'p_year': 'q2020_3'}, {'p_year': 'q2020_03'})

# y2020 행 삭제
company_all = company_all[company_all['p_year'] != 'y2020']

# %% 새 DB 생성 및 전체 종목 재무재표 저장

con1 = sqlite3.connect("c:/Develop/Data_science/stock_market/db/stock_market.db")
company_all.to_sql('fin_stmt', con1, if_exists='replace', index=False)

# 저장한 내용 조회
com_all = pd.read_sql("SELECT * FROM fin_stmt", con1, index_col=None)

# y2020 행 삭제
company_all = company_all[company_all['p_year'] != 'y2020']

# y2018, y2019 행 삭제
com_all = com_all[com_all['p_year'] != 'y2018']
com_all = com_all[com_all['p_year'] != 'y2019']

# %% name 컬럼에 특정 값 포함 된 경우 True/False 반환

if (table_list['name'].str.contains('C155661')).any():
	print("하나는 출석을 했습니다.")
else:
	print("하나 결석했습니다.")
    
table_list1 = table_list[table_list['name'].str.contains('C')]

# test git
