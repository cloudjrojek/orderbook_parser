# Script for translating xls orderbook to json understood by Coinbene Exchange API

import sys
import pandas as pd
import json
import datetime


def print_help():
    print(f'Usage: python orderbook_parser.py path_to_input_excel_file.xls')


def xls_to_json(xls_file_location: str) -> str:
    df = pd.read_excel(xls_file_location, usecols='A:C')
    df.dropna(how='all', inplace=True)
    df.rename(columns={df.columns[0]: 'direction', df.columns[1]: 'price', df.columns[2]: 'quantity'}, inplace=True)
    df['instrument_id'] = 'ELCASH/USDT'
    df['direction'] = df['direction'].replace({'BUY': 1, 'SELL': 2})

    return df.to_json(orient='records')


if __name__ == '__main__':
    if len(sys.argv) >= 2:
        orderbook_json = xls_to_json(sys.argv[1])
        json_string = json.dumps(orderbook_json, indent=4)
        print(orderbook_json)
        with open(f'{datetime.datetime.now().strftime("%Y%m%d_%H:%M")}_orderbook.json', 'w') as output:
            output.write(orderbook_json)
    else:
        print_help()

