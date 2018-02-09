from flask import Flask, request, jsonify
import payment
from paymentdb import Account, session

app = Flask(__name__)

@app.route("/create_user")
def create_user():
    username = request.args['username']
    for c in username:
        if not c.isdigit() and not c.isalpha():
            return jsonify({'error':'username must be alphanumeric'})
    payment.create_user(username)
    return jsonify({'success':'user created'})

@app.route("/get_address")
def get_address():
    username = request.args['username']
    address = payment.get_address(username)
    return jsonify({username: address})

@app.route("/withdraw")
def withdraw():
    username = request.args['username']
    balance = int(request.args['balance'])
    address = request.args['address']
    response = payment.withdraw(username, balance, address)
    if response:
        return jsonify(response)
    return jsonify({'success':'withdrawal sent'})

@app.route("/reset_data")
def reset_data():

    print('1GOT RESET REQUEST')
    session.query(Account).delete()
    print('2GOT RESET REQUEST')
    session.commit()
    print('3GOT RESET REQUEST')
    payment.create_user('hotwallet')
    print('4GOT RESET REQUEST')
    return ''
