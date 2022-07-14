import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
from datetime import datetime

BASE_URL = 'https://finance.naver.com/sise/sise_market_sum.naver?sosok='
CODES = [0, 1]
START_PAGE = 1
fields = []

now = datetime.now()
formattedDate = now.strftime("%Y%m%d")


def execute_crawler():
    df_total = []

    for code in CODES:

        res = requests.get(BASE_URL + str(CODES[0]))
        page_soup = BeautifulSoup(res.text, 'lxml')

        total_page_num = page_soup.select_one('td.pgRR > a')
        total_page_num = int(total_page_num.get('href').split('=')[-1])

        ipt_html = page_soup.select_one('div.subcnt_sise_item_top')

        global fields
        fields = [item.get('value') for item in ipt_html.select('input')]

        result = [crawler(code, str(page)) for page in range(1, total_page_num + 1)]

        df = pd.concat(result, axis=0, ignore_index=True)

        df_total.append(df)

    df_total = pd.concat(df_total)
    df_total.reset_index(inplace=True, drop=True)

    df_total.to_excel('NaverFinance.xlsx')

    return df_total


def crawler(code, page):

    global fields

    data = {'menu': 'market_sum',
            'fieldIds': fields,
            'returnUrl': BASE_URL + str(code) + "&page=" + str(page)}

    res = requests.post('https://finance.naver.com/sise/field_submit.naver', data=data)

    page_soup = BeautifulSoup(res.text, 'lxml')

    table_html = page_soup.select_one('div.box_type_l')

    header_data = [item.get_text().strip() for item in table_html.select('thead th')][1:-1]

    inner_data = [item.get_text().strip() for item in table_html.find_all(lambda x:
                                                                          (x.name == 'a' and 'tltle'
                                                                           in x.get('class', [])) or
                                                                          (x.name == 'td' and 'number'
                                                                           in x.get('class', []))
                                                                          )]

    no_data = [item.get_text().strip() for item in table_html.select('td.no')]
    number_data = np.array(inner_data)

    number_data.resize(len(no_data), len(header_data))

    df = pd.DataFrame(data=number_data, columns=header_data)
    return df


def get_universe():
    df = execute_crawler()

    mapping = {',': '', 'N/A': '0'}
    df.replace(mapping, regex=True, inplace=True)

    cols = ['거래량', '매출액', '매출액증가율', 'ROE', 'PER']

    df[cols] = df[cols].astype(float)

    df = df[(df['거래량'] > 0) & (df['매출액'] > 0) & (df['매출액증가율'] > 0) & (df['ROE'] > 0) & (df['PER'] > 0)
            & (~df.종목명.str.contains("지주")) & (~df.종목명.str.contains("홀딩스"))]

    df['1/PER'] = 1 / df['PER']

    df['RANK_ROE'] = df['ROE'].rank(method='max', ascending=False)

    df['RANK_1/PER'] = df['1/PER'].rank(method='max', ascending=False)

    df['RANK_VALUE'] = (df['RANK_ROE'] + df['RANK_1/PER']) / 2

    df = df.sort_values(by=['RANK_VALUE'])

    df.reset_index(inplace=True, drop=True)

    df = df.loc[:199]

    df.to_excel('universe.xlsx')
    return df['종목명'].tolist()


if __name__ == "__main__":
    print('Start!')
    universe = get_universe()
    print(universe)
    print('End')
