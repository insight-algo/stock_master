# 재무제표 하나의 테이블로 통합하는 버전

# 재무제표 크롤링 & DB 저장
import pandas as pd
import sqlite3
import time

# 누적저장 테이블 컬럼 정의, 누적 테이블(com_all) 컬럼 불러오기 stock_db_restructure.py
stock_all = pd.DataFrame(columns=list(com_all))

# 종목코드 가져오기
code_df = pd.read_html('http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13', header=0)[0]

# 종목코드가 6자리이기 때문에 6자리를 맞춰주기 위해 설정해줌
code_df.종목코드 = code_df.종목코드.map('{:06d}'.format)

# 우리가 필요한 것은 회사명과 종목코드이기 때문에 필요없는 column들은 제외해준다.
code_df = code_df[['회사명', '종목코드']]


url_tmpl = 'https://finance.naver.com/item/main.nhn?code=%s'

error_code = []
count = 0

for code in code_df['종목코드']:
    url = url_tmpl % (code)
    try:
        stock = pd.read_html(url, encoding='euc-kr')[3].droplevel([0,2], axis=1)
    except IndexError:
        error_code.append(code)
        continue
    stock.columns = ['info', 'y2018', 'y2019', 'y2020', 'y2021(E)', 'q2020_09', 
                        'q2020_12', 'q2021_03', 'q2021_06', 'q2021_09', 'q2021_12(E)']
    
    stock = stock.transpose()
    
    stock.rename(columns=stock.iloc[0],inplace=True)
    stock = stock.drop(stock.index[0])
    stock.reset_index(drop=False, inplace=True)
    stock.rename(columns={'index':'p_year'}, inplace=True)
    stock['company'] = 'C' + code
    
    stock_all = stock_all.append(stock)
    
    del[[stock]]
    
    count += 1
    if count % 10 == 0:
        print(count)
    time.sleep(3)


# %% 누적 수집 정보 통합 후 저장


con1 = sqlite3.connect("c:/Develop/Data_science/stock_market/db/stock_market.db")
stock_all.to_sql('stock_20220214', con1, if_exists='replace', index=False)


# %% 2107년 재무재표 테이블(com_all)과 2018 이후 테이블(stock_all) 합치기

com_all = com_all.append(stock_all)

com_all.to_sql('stock_2017_2019', con1, if_exists='replace', index=False)

# 특정 주식코드 조회
com_all.loc[com_all['company'] == 'C000210']
# %% 한종목 수집 후 구조 변환 테스트

code = '155660'

url_tmpl = 'https://finance.naver.com/item/main.nhn?code=%s'

url = url_tmpl % (code)
fin_stmt = pd.read_html(url, encoding='euc-kr')[3].droplevel([0,2], axis=1)

fin_stmt.columns = ['info', 'y2018', 'y2019', 'y2020', 'y2021(E)', 'q2020_09', 
                        'q2020_12', 'q2021_03', 'q2021_06', 'q2021_09', 'q2021_12(E)']

fin_stmt = fin_stmt.transpose()
        
fin_stmt.rename(columns=fin_stmt.iloc[0],inplace=True)
fin_stmt = fin_stmt.drop(fin_stmt.index[0])
fin_stmt.reset_index(drop=False, inplace=True)
fin_stmt.rename(columns={'index':'p_year'}, inplace=True)
fin_stmt['company'] = 'C' + code

del[[fin_stmt]]

