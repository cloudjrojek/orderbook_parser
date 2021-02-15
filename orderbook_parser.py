# Script for translating xls orderbook to json understood by Coinbene Exchange API

import sys
from typing import List
import pandas as pd
import json
import time
import datetime
import logging
from requests import Response

from apis.coinbene_v3 import (
get_account_info,
create_many_orders,
create_order,
cancel_many_orders,
cancel_order,
create_preorder,
)


# Set up logging
log = "orderbook_logger.log"
logging.basicConfig(filename=log,level=logging.DEBUG,format='%(asctime)s %(message)s', datefmt='%d/%m/%Y %H:%M:%S')


def print_help():
    print(f'Usage: python orderbook_parser.py "path_to_input_excel_file.xls" "TICKER/TICKER" True/False (automatic order execution)')


def xls_to_json(xls_file_location: str, ticker: str, slice_size: int = 10) -> List[str]:
    df = pd.read_excel(xls_file_location, usecols='A:C', nrows=100)
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
        slicesize = int(sys.argv[3]) if len(sys.argv) >= 4 else 10
        orderbook = xls_to_json(sys.argv[1], sys.argv[2], slicesize)
        filename = f'{datetime.datetime.now().strftime("%Y%m%d_%H:%M")}_orderbook.json'
        with open(filename, 'w') as output:
            logging.info(f'Input split into {len(orderbook)} chunks, {slicesize} orders per chunk')
            output.write(f'Input split into {len(orderbook)} chunks, {slicesize} orders per chunk')
            for index, chunk in enumerate(orderbook, start=1):
                logging.info(f'Chunk #{index}')
                logging.info(chunk.replace('\\', ''))
                output.write(f'\nChunk #{index}\n')
                output.write(chunk.replace('\\', ''))

        if len(sys.argv) >= 5 and sys.argv[4].upper() == 'TRUE':
            order_id_list = []
            for index, chunk in enumerate(orderbook, start=1):
                chunk = chunk.replace('\\', '')
                response = create_preorder(json.dumps(json.loads(chunk)[0]))
                rsp = response.json()
                logging.info(f'respose: {json.dumps(rsp)}')
                if not response.ok or int(rsp['code']) >= 400:
                    logging.error('!!! NOT OK RESPONSE !!!')
                    logging.error(chunk)
                    logging.error(f'returned status code: {response.status_code}')
                    logging.error(response.text)
                    break

                if int(rsp['code']) < 400 and rsp['data']['orderId']:
                    order_id_list.append(rsp['data']['orderId'])
                else:
                    logging.error(f'!!! NOT FULLY EXECUTED !!!')
                    logging.error(f'not all fragments were successfully placed')
                undo_payload_dict = {'orderIds': order_id_list}
                undo_payload = json.dumps(undo_payload_dict)
                logging.info(f'POST {chunk} returned')
                logging.info(json.dumps(rsp))
                logging.info(f"order ids: {order_id_list}")
                logging.info(f"to undo, cancel_many_orders with: {undo_payload}")
                if len(order_id_list) >= 10:
                    logging.info(f'clearing order id list')
                    logging.info(f"undo array: cancel_many_orders with: {undo_payload}")
                    order_id_list = []

                time.sleep(5)
                # logging.info(f'cancelling recently created orders (payload: {undo_payload}')
                # cancel_response = cancel_many_orders(undo_payload)
                # cancel_rsp = cancel_response.json()
                # logging.info(f'cancel_respose: {json.dumps(cancel_rsp)}')
                # time.sleep(5)
    else:
        print_help()

