import boto3
import web3

dynamodb = boto3.resource('dynamodb')

table = dynamodb.Table('UserBalances')

#Payment Loop

last_block = 1733063


addresses = {0x2fC0822D93E15F73C542d3faCABcbDCe328B9d9F: 'thabaptiser'}
web3 = web3.Web3(web3.HTTPProvider('http://54.238.99.37:8545'))

while True:
    most_recent_block = web3.eth.blockNumber - 5
    print(most_recent_block)
    for block_num in range(last_block, most_recent_block):
        print(block_num)
        block = web3.eth.getBlock(block_num, full_transactions=True)
        print(block)
        for t in block['transactions']:
            print(t)
            #raw_input()
            continue
            if t['to'] in addresses:
                username = addresses[t['to']]['username']
                table.update_item(
                    Key={
                        'UserIDandSymbol': '{username}.ETH'.format(username=username),
                        },
                    UpdateExpression='SET balance = balance + :val1',
                    ExpressionAttributeValues={
                        ':val1': t.amount
                        }
                )
                user_balance = table.get_item(
                    Key={
                        'UserIDandSymbol': '{username}.ETH'.format(username=username),
                        },
                )['item']
                print(user_balance['balance'])
    last_block = most_recent_block
