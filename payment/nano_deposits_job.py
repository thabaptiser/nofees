import boto3
import codecs
import sqlalchemy
from raiblocks import RPCClient
from paymentdb import session, create_tables, Session, NanoAccount
from threading import Thread

import os

print('connecting')
dynamodb = boto3.resource('dynamodb')
print('connecting')
table = dynamodb.Table('UserBalances')
print('connecting')
create_tables()
print('connecting')
rpc = RPCClient('http://54.238.79.10:7076')

#Accounts dict format {PUBLICKEY: USERNAME, BALANCE, LAST_BLOCK}

def get_accounts(wallet): #load accounts from SQL and add those to the Node wallet
    accounts = {}
    account_list = session.query(NanoAccount).all()
    loaded_accounts = rpc.account_list(wallet)
    balances = rpc.RPCClient.wallet_balances(wallet)
    for account in account_list:
        if account.account_id not in loaded_accounts:
            rpc.wallet_add(wallet, account.private_key)
        accounts[account.account_id] = (account.username, account.last_block)
    session.commit()
    return accounts

def send_to_hot_wallet(account):
    return

def update_account(account, wallet):
    if account.last_block == 0:
        account.last_block = rpc.account_info(account.account_id)['frontier']
    latest_block = rpc.account_history(account, count=1)[0]['hash']
    if latest_block != account.last_block:
        curr_block = account.last_block
        while next_block != latest_block:
            curr_block = rpc.chain(curr_block, count=1) #gets the next block
            block = rpc.block(curr_block)
            if block['type'] != 'receive':
                account.last_block = curr_block
                continue
            print("RECEIVED")
            amount = block['amount']
            add_balance(account.username, amount)
            account = session.query(NanoAccount).filter(NanoAccount.account_id == account_id).one()
            account.balance = rpc.account_balance(account.account_id)
            log(account, amount)
            send_to_hot_wallet(account)
            account.last_block = curr_block

def add_balance(username, amount):
    table.update_item(
         Key={'UserIDandSymbol': '{username}.ETH'.format(username=username)},
         UpdateExpression='SET Balance = Balance + :val1',
         ExpressionAttributeValues={':val1': value}
    )



def log(username, value):
     user_balance = table.get_item(
         Key={'UserIDandSymbol': '{username}.NANO'.format(username=username)},
     )['Item']

     if username == 'hotwallet':
         print('Deposited {amount} to hotwallet from {user}. Hotwallet Balance is now {balance}'.format(
             amount=value,
             user=username,
             balance=user_balance['Balance']))

     else:
         print('Deposited {amount} to {username}. Balance is now {balance}'.format(amount=value, username=username, balance=user_balance['Balance']))

def send_to_hot_wallet(username, wallet):
    hot_wallet_account = session.query(NanoAccount).filter(NanoAccount.username == 'hotwallet').one()
    source_account = session.query(NanoAccount).filter(NanoAccount.username == username).one()
    balance = rpc.account_balance(account.account_id)
    rpc.send(
            wallet = wallet,
            source = source_account.account_id,
            destination = hot_wallet_account.account_id,
            amount = balance,
            )


def nano_deposits():
    #session = Session()
    print('starting')
    wallet = rpc.wallet_create()
    while True:
        accounts = get_accounts(wallet)
        for account in accounts:
            update_account(account, wallet)
nano_deposits()
