import boto3
import codecs
import sqlalchemy
import web3
from paymentdb import session, create_tables, Session, Account
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

def create_address(username):
    private_key = utils.sha3(os.urandom(4096))
    raw_address = utils.privtoaddr(private_key)
    public_key = utils.checksum_encode(raw_address)
    new_account = Account(
            public_key = public_key,
            private_key = ''.join(chr(x) for x in private_key),
            balance = 0,
            username = username
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
        create_address(username)
        print('created user', username)
    except:
        print('could not create user', username)

def get_address(username):
    account = session.query(Account).filter(Account.username == username).one()
    return account.public_key

def send_to_hot_wallet(public_key):
    account = session.query(Account).filter(Account.public_key == public_key).one()
    hotwallet = session.query(Account).filter(Account.username == 'hotwallet').first()
    gasprice = web3.toWei(1, 'Gwei')
    startgas = 21000
    tx = Transaction(
	nonce=web3.eth.getTransactionCount(account.public_key),
	gasprice=gasprice,
	startgas=startgas,
	to=hotwallet.public_key,
	value=web3.toWei(account.balance, 'szabo') - gasprice * startgas,
        data=b'',
    )
    tx.sign(bytes(ord(x) for x in account.private_key))
    raw_tx = rlp.encode(tx)
    raw_tx_hex = web3.toHex(raw_tx)
    web3.eth.sendRawTransaction(raw_tx_hex)
