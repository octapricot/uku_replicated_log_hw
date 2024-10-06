from flask import Flask, request, jsonify
import logging
import requests

app = Flask(__name__)

messages = [] 

secondaries = ["http://secondary1:5001", "http://secondary2:5001"] 

logging.basicConfig(level=logging.INFO) 

@app.route('/append', methods=['POST'])
def append_message(): 
    msg = request.json.get('message')
    messages.append(msg)
    logging.info(f"Received message: {msg}")

    for secondary in secondaries: 
        try:
            res = requests.post(f"{secondary}/replicate", json={"message": msg})
            if res.status_code == 200:
                logging.info(f"Secondary {secondary} acknowledged.")
            else: 
                return jsonify({"error": "Failed to replicate"}), 500
        except requests.exceptions.RequestException as exc:
            logging.error(f"Error while communicating with secondary: {exc}")
            return jsonify({"error": "Secondary cannot be reached"}), 500
    return jsonify({"status": "Message appended and replicated"}), 200

@app.route('/messages', methods=['GET'])
def get_messages():
    return jsonify(messages)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
