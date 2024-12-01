from flask import Flask, request, jsonify
import logging
#import requests
import asyncio 
from concurrent.futures import ThreadPoolExecutor
import aiohttp 

app = Flask(__name__)

messages = [] 

secondaries = ["http://secondary1:5001", "http://secondary2:5001"] 

logging.basicConfig(level=logging.INFO) 

async def replicate_to_secondary(secondary_url, message):
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(f"{secondary_url}/replicate", json={"message": message}) as response:
                if response.status == 200:
                    logging.info(f"Secondary {secondary_url} acknowledged.")
                else:
                    logging.error(f"Failed to replicate to {secondary_url}. Status: {response.status}")
        except aiohttp.ClientError as e:
            logging.error(f"Error communicating with secondary {secondary_url}: {e}")

@app.route('/append', methods=['POST'])
def append_message(): 
    msg = request.json.get('message')
    messages.append(msg)
    logging.info(f"Received message: {msg}")

    #for secondary in secondaries: 
    #    try:
    #        res = requests.post(f"{secondary}/replicate", json={"message": msg})
    #        if res.status_code == 200:
    #            logging.info(f"Secondary {secondary} acknowledged.")
    #        else: 
    #            return jsonify({"error": "Failed to replicate"}), 500
    #    except requests.exceptions.RequestException as exc:
    #        logging.error(f"Error while communicating with secondary: {exc}")
    #        return jsonify({"error": "Secondary cannot be reached"}), 500
    
    # Asynchronous replication to all secondaries
    async def replicate():
        tasks = [replicate_to_secondary(secondary, msg) for secondary in secondaries]
        print("Sent replication commands. Waiting for finish.")
        await asyncio.gather(*tasks)
    
    # Run asynchronous replication in the background
    asyncio.run(replicate())

    return jsonify({"status": "Message appended and replicated"}), 200

@app.route('/messages', methods=['GET'])
def get_messages():
    return jsonify(messages)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
