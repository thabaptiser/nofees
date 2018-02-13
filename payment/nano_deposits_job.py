import boto3
import codecs
import sqlalchemy
from raiblocks import RPCClient, convert
from paymentdb import session, create_tables, Session, NanoAccount
from threading import Thread

import os

print('connecting to dynamodb')
dynamodb = boto3.resource('dynamodb')
print('connecting to RDS')
table = dynamodb.Table('UserBalances')
create_tables()
print('connecting to Nano Node')
rpc = RPCClient('http://54.238.79.10:7076')

def get_accounts(wallet): #load accounts from SQL and add those to the Node wallet
    accounts = session.query(NanoAccount).all()
    loaded_accounts = rpc.account_list(wallet)
    #print('loaded accounts', loaded_accounts)
    balances = rpc.wallet_balances(wallet)
    for account in accounts:
        if account.account_id not in loaded_accounts:
            rpc.wallet_add(wallet, account.private_key)
            print('Added to wallet:', account.private_key)
    session.commit()
    return accounts

def send_to_hot_wallet(account):
    return

def initialize_account(account, wallet):
    if account.username != 'hotwallet':
        hotwallet_account = session.query(NanoAccount).filter(NanoAccount.username == 'hotwallet').one()
        rpc.send(
            wallet = wallet,
            source = hotwallet_account.account_id,
            destination = account.account_id,
            amount = 0,
        )
    pending = rpc.accounts_pending([account.account_id], count = 10, threshold=0)
    #print('Pending:', pending)
    for account_id, blocks in pending.items():
        for block in blocks:
            rpc.receive(wallet, account_id, block)
    account.last_block = rpc.account_info(account.account_id)['frontier']
    session.commit()
    return

def get_blocks_between(block1, block2):
    ret_blocks = []
    ret_blocks.append(block2)
    while True:
        temp_blocks = rpc.chain(block2, count=10)
        #print("CHAIN starting at {b}: {c}".format(b=block2, c=temp_blocks))
        if len(temp_blocks) == 1:
            raise Exception
        for b in temp_blocks[1:]:
            if b == block1:
                return ret_blocks[::-1]
            ret_blocks.append(b)
        block2 = b

def update_account(account, wallet):
    if account.last_block == '0':
                #rpc.block_create('open', account=account.account_id, wallet=wallet)
        initialize_account(account, wallet)
    #print(account.last_block)
    latest_block = rpc.account_info(account.account_id)['frontier']
    #print(account.account_id, rpc.account_info(account.account_id))
    #print('latest_block', latest_block)
    if latest_block != account.last_block:
        blocks = get_blocks_between(account.last_block, latest_block)
        #print(blocks)
        for curr_block in blocks:
            #print(latest_block, curr_block)
            #print('block:', curr_block)
            #print(frontier)
            block = rpc.blocks_info([curr_block])[curr_block]
            #print(block)
            if block['contents']['type'] != 'receive':
                #print('type', block['type'])
                account.last_block = curr_block
                session.commit()
                continue
            if not block.get('amount', False):
                print('amount was 0 for block', curr_block)
                account.last_block = curr_block
                session.commit()
                continue
            print("RECEIVED")
            #print(curr_block, block)
            amount = convert(block['amount'], from_unit='raw', to_unit='xrb')
            add_balance(account.username, amount)
            account.balance = int(convert(rpc.account_balance(account.account_id)['balance'], from_unit='raw', to_unit='krai'))
            log(account, amount)
            send_to_hot_wallet(account, wallet)
            account.last_block = curr_block
            session.commit()

def add_balance(username, amount):
    #print('{username}.NANO'.format(username=username))
    table.update_item(
         Key={'UserIDandSymbol': '{username}.NANO'.format(username=username)},
         UpdateExpression='SET Balance = Balance + :val1',
         ExpressionAttributeValues={':val1': amount}
    )



def log(account, value):
     user_balance = table.get_item(
         Key={'UserIDandSymbol': '{username}.NANO'.format(username=account.username)},
     )['Item']

     if account.username == 'hotwallet':
         print('Deposited {amount} to hotwallet. Hotwallet Balance is now {balance}'.format(
             amount=value,
             user=account.username,
             balance=user_balance['Balance']))

     else:
         print('Deposited {amount} to {username}. Balance is now {balance}'.format(amount=value, username=account.username, balance=user_balance['Balance']))

def send_to_hot_wallet(account, wallet):
    if account.username == 'hotwallet':
        return
    hotwallet_account = session.query(NanoAccount).filter(NanoAccount.username == 'hotwallet').one()
    balance = rpc.account_balance(account.account_id)['balance']
    rpc.send(
            wallet = wallet,
            source = account.account_id,
            destination = hotwallet_account.account_id,
            amount = balance,
            )

def receive_all(wallet, accounts):
    pending = rpc.accounts_pending([account.account_id for account in accounts], count = 10, threshold=0)
    #print('Pending:', pending)
    for account, blocks in pending.items():
        for block in blocks:
            rpc.receive(wallet, account, block)


def nano_deposits():
    #session = Session()
    print('starting deposits job')
    wallet = rpc.wallet_create()
    while True:
        accounts = get_accounts(wallet)
        #print(rpc.wallet_republish(wallet, 1000))
        for account in accounts:
            receive_all(wallet, accounts)
            #print(accounts)
            update_account(account, wallet)
        print('loop')
nano_deposits()
