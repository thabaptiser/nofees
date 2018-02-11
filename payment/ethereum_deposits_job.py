import boto3
import codecs
import sqlalchemy
import web3
from paymentdb import session, create_tables, Session, EthereumAccount
from ethereum import utils
import rlp
from ethereum.transactions import Transaction
from threading import Thread


import os


dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('UserBalances')

create_tables()
web3 = web3.Web3(web3.HTTPProvider('http://54.238.99.37:8545'))


# Adresses dict format {ADDRESS: USERNAME, LAST_BLOCK}



def get_addresses(addresses):
    rows = session.query(EthereumAccount).count()
    if rows != len(addresses):
        addresses = {}
        account_list = session.query(EthereumAccount).all()
        for account in account_list:
            addresses[account.public_key] = (account.username, last_block)
    return addresses

def update_address(public_key, last_block):
    account = session.query(EthereumAccount).filter(EthereumAccount.public_key == public_key).one()
    account.balance = web3.fromWei(web3.eth.getBalance(account.public_key), 'szabo')
    account.last_block = last_block
    session.commit()

def send_to_hot_wallet(public_key):
    account = session.query(EthereumAccount).filter(EthereumAccount.public_key == public_key).one()
    hotwallet = session.query(EthereumAccount).filter(EthereumAccount.username == 'hotwallet').first()
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

def withdraw(username, amount, address):
    user_balance = table.get_item(
        Key={'UserIDandSymbol': '{username}.ETH'.format(username=username)},
    )['Item']
    account = session.query(EthereumAccount).filter(EthereumAccount.username == username).first()
    hotwallet = session.query(EthereumAccount).filter(EthereumAccount.username == 'hotwallet').first()
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
    tx.sign(bytes(ord(x) for x in hotwallet.private_key))
    raw_tx = rlp.encode(tx)
    raw_tx_hex = web3.toHex(raw_tx)
    web3.eth.sendRawTransaction(raw_tx_hex)
    table.update_item(
        Key={'UserIDandSymbol': '{username}.ETH'.format(username=username),},
        UpdateExpression='SET Balance = Balance - :val1',
        ExpressionAttributeValues={':val1': amount}
    )

    print('Withdrew {amount} from {user} to address {address}'.format(amount=amount, user=username, address=address))

def add_balance(username, value):
    table.update_item(
        Key={'UserIDandSymbol': '{username}.ETH'.format(username=username)},
        UpdateExpression='SET Balance = Balance + :val1',
        ExpressionAttributeValues={':val1': value}
    )

def log(username, value):
    user_balance = table.get_item(
        Key={'UserIDandSymbol': '{username}.ETH'.format(username=username)},
    )['Item']

    if username == 'hotwallet':
        print('Deposited {amount} to hotwallet from {user}. Hotwallet Balance is now {balance}'.format(
            amount=value,
            user=username,
            balance=user_balance['Balance']))

    else:
        print('Deposited {amount} to {username}. Balance is now {balance}'.format(amount=value, username=username, balance=user_balance['Balance']))



def get_last_block():
    with open("lastblock") as f:
        last_block = int(f.read())
        return last_block

def ethereum_deposits():
    #session = Session()
    last_block = get_last_block()
    addresses = get_addresses({})
    print('starting')
    while True:
        most_recent_block = web3.eth.blockNumber
        print(most_recent_block)
        for block_num in range(last_block, most_recent_block):
            print(block_num)
            block = web3.eth.getBlock(block_num, full_transactions=True)
            addresses = get_addresses(addresses)
            for t in block['transactions']:
                if addresses[t['to']].last_block == block_num: #skip updating address if we've already looked at it in this block
                    continue
                if t['to'] in addresses:
                    update_address(t['to'], block_num)
                    value = int(web3.fromWei(t['value'], 'szabo'))
                    username = addresses[t['to']]
                    add_balance(username, value)
                    log(username, value)
                    if username != 'hotwallet':
                        send_to_hot_wallet(t['to'])
        last_block = most_recent_block
ethereum_deposits()
