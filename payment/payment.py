import boto3
import codecs
import sqlalchemy
import web3
from paymentdb import session, create_tables, NanoAccount, EthereumAccount
from ethereum import utils
from raiblocks import RPCClient
from flask import Flask
import rlp
from ethereum.transactions import Transaction
from threading import Thread


import os


dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('UserBalances')

create_tables()
web3 = web3.Web3(web3.HTTPProvider('http://54.238.99.37:8545'))
rpc = RPCClient('http://54.238.79.10:7076')

def create_address_ethereum(username):
    private_key = utils.sha3(os.urandom(4096))
    raw_address = utils.privtoaddr(private_key)
    public_key = utils.checksum_encode(raw_address)
    new_account = EthereumAccount(
            public_key = public_key,
            private_key = private_key,
            balance = 0,
            username = username
            )
    session.add(new_account)
    session.commit()
    print('created ethereum address')

def create_address_nano(username):
    keys = rpc.key_create()
    new_account = NanoAccount(
            public_key = keys['public'],
            private_key = keys['private'],
            account = keys['account'],
            balance = 0,
            username = username,
            last_block = 0,
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
    create_user_balance(username)
    create_address_ethereum(username)
    create_address_nano(username)
    print('created user', username)

def get_address(username):
    eth_addr = session.query(EthereumAccount).filter(EthereumAccount.username == username).one()
    nano_addr = session.query(NanoAccount).filter(NanoAccount.username == username).one()
    return {'ETH': eth_addr.public_key, 'NANO': nano_addr.account_id}

def withdraw(username, amount, address):
    user_balance = table.get_item(
        Key={
            'UserIDandSymbol': '{username}.ETH'.format(username=username),
            },
        )['Item']
    account = session.query(Account).filter(Account.username == username).first()
    hotwallet = session.query(Account).filter(Account.username == 'hotwallet').first()
    gasprice = web3.toWei(10, 'Gwei')
    startgas = 21000
    print('user balance', user_balance['Balance'])
    print('amount', amount, 'withdraw fee', web3.fromWei(gasprice * startgas, 'szabo'))
    if amount <= 0:
        return {'error': 'You can not withdraw 0 or a negative amount'}
    if amount > user_balance['Balance']:
        return {'error': 'You can not withdraw more than your available balance'}
    if web3.toWei(amount, 'szabo') <= gasprice * startgas:
        return {'error': 'You can not withdraw less than the withdrawal fee'}
    tx = Transaction(
	nonce=web3.eth.getTransactionCount(hotwallet.public_key),
	gasprice=gasprice,
	startgas=startgas,
	to=address,
	value=web3.toWei(amount, 'szabo') - gasprice * startgas,
        data=b'',
    )
    tx.sign(bytes(private_key))
    raw_tx = rlp.encode(tx)
    raw_tx_hex = web3.toHex(raw_tx)
    web3.eth.sendRawTransaction(raw_tx_hex)
    table.update_item(
    Key={
        'UserIDandSymbol': '{username}.ETH'.format(username=username),
        },
        UpdateExpression='SET Balance = Balance - :val1',
        ExpressionAttributeValues={
            ':val1': amount
        }
    )

    print('Withdrew {amount} from {user} to address {address}'.format(amount=amount, user=username, address=address))


