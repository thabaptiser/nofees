import boto3
import codecs
import sqlalchemy
import web3
import decimal as dc
from paymentdb import session, create_tables, NanoAccount, EthereumAccount
from ethereum import utils
from raiblocks import RPCClient, convert
from flask import Flask, jsonify
import rlp
from ethereum.transactions import Transaction
from threading import Thread

import values


import os


dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('UserBalances')

create_tables()
web3 = web3.Web3(web3.HTTPProvider(values.eth_node))
rpc = RPCClient(values.nano_node)

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
    for c in values.currencies:
        user_balance = table.get_item(Key={'UserIDandSymbol': '{username}.{c}'.format(username=username, c=c)})
        print(user_balance)
        if 'Item' not in user_balance:
            table.put_item(Item={'UserIDandSymbol': '{username}.{c}'.format(username=username, c=c), 'Balance': 0})


def create_user(username):
    create_user_balance(username)
    create_address_ethereum(username)
    create_address_nano(username)
    print('created user', username)

def get_address(username):
    eth_addr = session.query(EthereumAccount).filter(EthereumAccount.username == username).one()
    nano_addr = session.query(NanoAccount).filter(NanoAccount.username == username).one()
    return {'ETH': eth_addr.public_key, 'NANO': nano_addr.account_id}

def withdraw_eth(username, amount, address):
    user_balance = table.get_item(Key={'UserIDandSymbol': '{username}.ETH'.format(username=username)})['Item']
    account = session.query(Account).filter(Account.username == username).first()
    hotwallet = session.query(Account).filter(Account.username == 'hotwallet').first()
    gasprice = web3.toWei(10, 'Gwei')
    startgas = 21000
    print('user balance', user_balance['Balance'])
    print('amount', amount, 'withdraw fee', web3.fromWei(gasprice * startgas, values.eth_base_unit))
    if amount == 'all':
        amount = user_balance['Balance']
    if amount <= 0:
        return {'success': False, 'error': 'You can not withdraw 0 or a negative amount'}
    if amount > user_balance['Balance']:
        return {'success': False, 'error': 'You can not withdraw more than your available balance'}
    if web3.toWei(amount, values.eth_base_unit) <= gasprice * startgas:
        return {'success': False, 'error': 'You can not withdraw less than the withdrawal fee'}
    tx = Transaction(
	nonce=web3.eth.getTransactionCount(hotwallet.public_key),
	gasprice=gasprice,
	startgas=startgas,
	to=address,
	value=web3.toWei(amount, eth_base_unit) - gasprice * startgas,
        data=b'',
    )
    tx.sign(bytes(private_key))
    raw_tx = rlp.encode(tx)
    raw_tx_hex = web3.toHex(raw_tx)
    tx_id = web3.eth.sendRawTransaction(raw_tx_hex)
    table.update_item(Key={'UserIDandSymbol': '{username}.ETH'.format(username=username)},
        UpdateExpression='SET Balance = Balance - :val1',
        ExpressionAttributeValues={':val1': amount})
    return {'success': True, 'error': None, 'tx_id': tx_id}
    print('Withdrew {amount} from {user} to address {address}'.format(amount=amount, user=username, address=address))


def withdraw_nano(username, amount, address):
    amount = dc.Decimal(amount)
    if amount <= 0:
        return {'success': False, 'error': 'You can not withdraw 0 or a negative amount'}
    hotwallet_account = session.query(NanoAccount).filter(NanoAccount.username == 'hotwallet').one()
    user_balance = table.get_item(Key={'UserIDandSymbol': '{username}.NANO'.format(username=username)})['Item']
    print(rpc.account_balance(hotwallet_account.account_id)['balance'])
    if convert(user_balance['Balance'], from_unit=values.nano_base_unit, to_unit='XRB') >= amount:

        table.update_item(Key={'UserIDandSymbol': '{username}.NANO'.format(username=username)},
            UpdateExpression='SET Balance = Balance - :val1',
            ExpressionAttributeValues={':val1': convert(amount, from_unit='XRB', to_unit=values.nano_base_unit)})

        tx_id = rpc.send(
                wallet = values.nano_wallet,
                source = hotwallet_account.account_id,
                destination = address,
                amount = '{0:f}'.format(convert(amount, from_unit='XRB', to_unit='raw'))
                )
        return {'success': True, 'error': None, 'tx_id': tx_id}
    else:
        return {'success': False, 'error': 'You can not withdraw more than your available balance'}

def withdraw(username, amount, address, currency):
    withdraw_functions = {'ETH': withdraw_eth, 'NANO': withdraw_nano}
    ret_dict = withdraw_functions[currency](username, amount, address)
    return ret_dict
