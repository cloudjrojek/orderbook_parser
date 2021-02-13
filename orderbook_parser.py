# Script for translating xls orderbook to json understood by Coinbene Exchange API

import sys
from typing import List
import pandas as pd
import numpy as np
import json
import datetime


def print_help():
    print(f'Usage: python orderbook_parser.py "path_to_input_excel_file.xls" "TICKER/TICKER"')


def xls_to_json(xls_file_location: str, ticker: str, slice_size: int = 10) -> List[str]:
    df = pd.read_excel(xls_file_location, usecols='A:C')
    df.dropna(how='all', inplace=True)
    df.rename(columns={df.columns[0]: 'direction', df.columns[1]: 'price', df.columns[2]: 'quantity'}, inplace=True)
    df['instrument_id'] = ticker
    df['direction'] = df['direction'].replace({'BUY': 1, 'SELL': 2})
    df_slices = [df.iloc[n:n+slice_size, :]
                 for n in range(0, len(df), slice_size)]
    order_list = [split_frame.to_json(orient='records') for split_frame in df_slices]
    return order_list


if __name__ == '__main__':
    if len(sys.argv) >= 3:
        slicesize = sys.argv[3] if len(sys.argv) >= 4 else 10
        orderbook = xls_to_json(sys.argv[1], sys.argv[2], slicesize)
        filename = f'{datetime.datetime.now().strftime("%Y%m%d_%H:%M")}_orderbook.json'
        with open(filename, 'w') as output:
            output.write(f'Input split into {len(orderbook)} chunks, {slicesize} orders per chunk')
            for index, chunk in enumerate(orderbook, start=1):
                output.write(f'\nChunk #{index}\n')
                output.write(chunk.replace('\\', ''))
        with open(filename, 'r') as output:
            print(output.read())
    else:
        print_help()

