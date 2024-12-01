from flask import Flask, request, jsonify
import logging
import time

app = Flask(__name__)

messages = []

logging.basicConfig(level=logging.INFO) 

@app.route('/replicate', methods=['POST'])
def replicate_message():
    msg = request.json.get('message')
    logging.info(f"Received replication request: {msg}")
    time.sleep(5)
    messages.append(msg)
    #logging.info(f"Replicated message: {msg}")
    logging.info("Replicated message appended.")
    return jsonify({"status": "Replicated"}), 200

@app.route('/messages', methods=['GET'])
def get_messages():
    return jsonify(messages)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
