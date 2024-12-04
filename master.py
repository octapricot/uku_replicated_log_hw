from flask import Flask, request, jsonify
import logging
import requests
import asyncio
import aiohttp
from latch import AsyncCountDownLatch

app = Flask(__name__)

messages = [] 

secondaries = ["http://secondary1:5001", "http://secondary2:5001"] 

logging.basicConfig(level=logging.INFO) 

# Послідовність id, щоб кожному новому меседжу присвоювати id + 1
message_id_seq = 0

def get_next_id():
    global message_id_seq
    message_id_seq += 1
    return message_id_seq

async def replicate_to_secondary(secondary_url: str, message: object, latch: AsyncCountDownLatch):
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(f"{secondary_url}/replicate", json=message) as response:
                if response.status == 200:
                    json_response = await response.json()
                    logging.info(f"Secondary {secondary_url} acknowledged. Replication status: {json_response.get('status', 'No status field')}")
                    await latch.count_down()
                else:
                    logging.error(f"Failed to replicate to {secondary_url}. Status: {response.status}")
        except aiohttp.ClientError as e:
            logging.error(f"Error communicating with secondary {secondary_url}: {e}")
            
async def replicate(write_concern: int, message: object):
    latch = AsyncCountDownLatch(write_concern - 1)

    # Запускає реплікацію
    for secondary in secondaries:
        asyncio.create_task(replicate_to_secondary(secondary, message, latch))

    # Чекає онулення
    try:
        await asyncio.wait_for(latch.await_latch(), timeout=3600)
        logging.info(f"Write concern {write_concern} satisfied.")
    except asyncio.TimeoutError:
        logging.error("Write concern not met within timeout.")
        
        
@app.route('/append', methods=['POST'])
def append_message(): 
    message_text = request.json.get('message')
    write_concern = request.json.get('write_concern', len(secondaries) + 1)
    if write_concern < 1 or write_concern > len(secondaries) + 1:
        return jsonify({"status": f"Invalid write_concern value {write_concern}"}), 400
    
    message = {"message_id": get_next_id(), "message": message_text}
    messages.append(message)
    logging.info(f"Received message: {message_text}, write concern: {write_concern}")
    
    asyncio.run(replicate(write_concern, message))
    
    return jsonify({"status": "Message appended and replicated"}), 200

@app.route('/messages', methods=['GET'])
def get_messages():
    return jsonify(messages)


# Ендпойнт для полегшення тестування (обнуляє всі повідомлення і тестові затримки на мастері й другорядних нодах)
@app.route('/test/reset', methods=['POST'])
def reset_servers():
    global messages, message_id_seq
    messages = []
    message_id_seq = 0
    for secondary in secondaries: 
        try:
            requests.post(f"{secondary}/test/reset")
        except requests.exceptions.RequestException as exc:
            logging.error(f"Error while resetting secondary: {exc}")
            return jsonify({"error": "Secondary cannot be reached"}), 500
    return "Success", 200
        

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
