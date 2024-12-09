from flask import Flask, request, jsonify
import logging
import requests
import asyncio
import aiohttp
from latch import AsyncCountDownLatch

app = Flask(__name__)

messages = [] 

secondaries = ["http://secondary1:5001", "http://secondary2:5001"] 

secondary_statuses = {secondary: "Unhealthy" for secondary in secondaries}
message_id_seq = 0

logging.basicConfig(level=logging.INFO) 

# Послідовність id, щоб кожному новому меседжу присвоювати id + 1
message_id_seq = 0

def get_next_id():
    global message_id_seq
    message_id_seq += 1
    return message_id_seq

# Quorum
def is_quorum_met(write_concern):
    healthy_count = sum(1 for status in secondary_statuses.values() if status == "Healthy")
    return healthy_count >= write_concern - 1

async def replicate_to_secondary(secondary_url: str, message: object, latch: AsyncCountDownLatch, retries=3, backoff_factor=2):
    attempt = 0
    while attempt < retries:
        attempt += 1
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(f"{secondary_url}/replicate", json=message) as response:
                    if response.status == 200:
                        json_response = await response.json()
                        logging.info(f"Secondary {secondary_url} acknowledged. Replication status: {json_response.get('status', 'No status field')}")
                        await latch.count_down()
                        return
                    else:
                        logging.error(f"Failed to replicate to {secondary_url}. Status: {response.status}")
        except aiohttp.ClientError as e:
            logging.error(f"Error communicating with secondary {secondary_url}: {e}")

        delay = backoff_factor ** attempt 
        logging.info(f"Retrying after {delay}s...")
        await asyncio.sleep(delay)
    
    logging.error(f"Failed to replicate to {secondary_url} after {retries} retries.")

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

# Backfill 
def backfill_secondary(secondary_url):
    for message in messages:
        try:
            requests.post(f"{secondary_url}/replicate", json=message)
        except requests.exceptions.RequestException as exc:
            logging.error(f"Failed to backfill message {message['message_id']} to {secondary_url}: {exc}")

# Heartbeat
async def monitor_heartbeats():
    while True:
        for secondary in secondaries:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(f"{secondary}/heartbeat") as response:
                        if response.status == 200 and secondary_statuses[secondary] != "Healthy":
                            logging.info(f"{secondary} is now Healthy.")
                            secondary_statuses[secondary] = "Healthy"
                        elif response.status != 200:
                            logging.warning(f"{secondary} is Suspected. Status: {response.status}")
                            secondary_statuses[secondary] = "Suspected"
            except aiohttp.ClientError as e:
                logging.error(f"Error checking heartbeat for {secondary}: {e}")
                secondary_statuses[secondary] = "Unhealthy"
        await asyncio.sleep(5) # Heartbeat interval   
        
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
        
# /health endpoint for monitoring the secondaries
@app.route('/health', methods=['GET'])
def get_health():
    logging.info("hello world")
    try:
        logging.info(f"Returning health statuses: {secondary_statuses}")
        if not secondary_statuses:
            raise ValueError("secondary_statuses is empty or not initialized")
        return jsonify(secondary_statuses), 200
    except Exception as e:
        logging.error(f"Error in /health endpoint: {e}")
        return jsonify({"error": "Internal server error"}), 500

if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.create_task(monitor_heartbeats())
    app.run(host='0.0.0.0', port=5000)
