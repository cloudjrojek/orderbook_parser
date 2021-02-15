import hashlib
import hmac
import functools
import os
import datetime
import requests


BASE_URL = 'https://openapi-exchange.coinbene.com'
API_KEY = os.getenv('API_KEY')
API_SECRET = os.getenv('API_SECRET')


def signed_post(func):
    @functools.wraps(func)
    def wrapper_decorator(*args, **kwargs) -> requests.Response:
        endpoint, body = func(*args, **kwargs)
        headers = kwargs.get('headers', {})
        timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        sign_msg = timestamp + 'POST' + endpoint + body
        signed_msg = sign(sign_msg, API_SECRET)
        headers['ACCESS-KEY'] = API_KEY
        headers['ACCESS-SIGN'] = signed_msg
        headers['ACCESS-TIMESTAMP'] = timestamp
        headers['Content-Type'] = 'application/json'
        response = requests.post(BASE_URL + endpoint, data=body, headers=headers)
        print(response.json())
        return response
    return wrapper_decorator

def signed_get(func):
    @functools.wraps(func)
    def wrapper_decorator(*args, **kwargs) -> requests.Response:
        endpoint = func(*args, **kwargs)
        headers = kwargs.get('headers', {})
        timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        sign_msg = timestamp + 'GET' + endpoint
        signed_msg = sign(sign_msg, API_SECRET)
        headers['ACCESS-KEY'] = API_KEY
        headers['ACCESS-SIGN'] = signed_msg
        headers['ACCESS-TIMESTAMP'] = timestamp
        headers['Content-Type'] = 'application/json'
        response = requests.get(BASE_URL + endpoint, headers=headers)
        print(response.json())
        return response
    return wrapper_decorator


def sign(message, secret):
    secret = secret.encode('utf-8')
    message = message.encode('utf-8')
    sign = hmac.new(secret, message, digestmod=hashlib.sha256).hexdigest()
    return sign


@signed_get
def get_account_info() -> str:
    return '/api/v3/spot/account/list'


@signed_post
def create_order(order: dict):
    return '/api/v3/spot/order', order


@signed_post
def create_many_orders(orders: dict):
    return '/api/v3/spot/batch_order', orders


@signed_post
def create_preorder(order: dict):
    return '/api/v3/spot/beforehand/place', order


@signed_post
def cancel_many_orders(body: dict):
    return '/api/v3/spot/batch_cancel_order', body


@signed_post
def cancel_order(order_id: str):
    body = {
        'order_id': order_id
    }
    return '/api/v3/spot/cancel_order', body


if __name__ == '__main__':
    response = get_account_info()


import unittest


class TestUtil(unittest.TestCase):
    def test_sign(self):
        sn = sign("2019-05-25T03:20:30.362ZGET/api/swap/v2/account/info", "9daf13ebd76c4f358fc885ca6ede5e27")
        self.assertEqual(sn, "a02a6428bb44ad338d020c55acee9dd40bbcb3d96cbe3e48dd6185e51e232aa2")

