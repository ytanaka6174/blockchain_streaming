"""
    This is an example web app that receives webhooks from node provider services like alchemy, infura, etc. 
    It's an easy way to get data but will require you to set up the webhooks with those services
    An ngrok instance is setup in the project, so the webhooks should point towards ngrok when setting it up
"""

from flask import Flask, request, abort
from kafka.producer import send_message
import json

app = Flask(__name__)

@app.route('/webhook', methods=['GET', 'POST'])
def webhook():
    if request.method == 'POST':
        print(request.json)
        value = json.dumps(request.json).encode('utf-8')
        send_message(topic='transaction', value=value)
        return 'success', 200
    elif request.method == 'GET':
        # Alchemy initially sends a GET request for verification
        return 'ok - GET verification', 200
    else:
        abort(400)


if __name__ == '__main__':
    app.run()
