import boto3
import codecs
import sqlalchemy
import web3
from paymentdb import session, create_tables, EthereumAccount
from ethereum import utils
from flask import Flask
import rlp
from ethereum.transactions import Transaction
from threading import Thread


import os


dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('UserBalances')

create_tables()
web3 = web3.Web3(web3.HTTPProvider('http://54.238.99.37:8545'))

def create_address_ethereum(username):
    private_key = utils.sha3(os.urandom(4096))
    raw_address = utils.privtoaddr(private_key)
    public_key = utils.checksum_encode(raw_address)
    new_account = EthereumAccount(
            public_key = public_key,
            private_key = ''.join(chr(x) for x in private_key),
            balance = 0,
            username = username
            )
    session.add(new_account)
    session.commit()

def create_address_nano(username):
    keys = rpc.key_create()
    new_account = NanoAccount(
            public_key = keys['public'],
            private_key = keys['private'],
            account = keys['account'],
            balance = 0,
            username = username,
            )
    session.add(new_account)
    session.commit()


def create_user_balance(username):
    user_balance = table.get_item(
        Key={
        'UserIDandSymbol': '{username}.ETH'.format(username=username),
            },
        )
    print(user_balance)
    if 'Item' not in user_balance:
        table.put_item(
            Item={
                'UserIDandSymbol': '{username}.ETH'.format(username=username),
                'Balance': 0
                }
        )

def create_user(username):
    try:
        create_user_balance(username)
        create_address_ethereum(username)
        create_address_nano(username)
        print('created user', username)
    except:
        print('could not create user', username)

def get_address(username):
    account = session.query(Account).filter(Account.username == username).one()
    return account.public_key
