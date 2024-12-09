from flask import Flask, request, jsonify
import logging
import time
import queue
import asyncio
import random 

app = Flask(__name__)

messages = []
last_processed_id = 0
message_buffer = {}

test_delay_seconds_queue = queue.Queue()

logging.basicConfig(level=logging.INFO) 

# Для тестування (перевіряє, чи були створені затримки виконання через ендпойнт /test/delay)
def delay_if_required():
    if not test_delay_seconds_queue.empty():
        delay_seconds = test_delay_seconds_queue.get()
        logging.info(f"Going to wait for {delay_seconds} seconds")  
        time.sleep(delay_seconds)

# Додає thread safety 
shared_lock = asyncio.Lock()

@app.route('/replicate', methods=['POST'])
async def replicate_message():
    # Simulate random internal server error 
    if random.random() < 0.5:
        return jsonify({"status": "Error"}), 500
    global last_processed_id, message_buffer
    data = request.json
    logging.info(f"Received replication request: {data}")
    message = data.get('message')
    message_id = data.get('message_id')
    
    delay_if_required()
    
    async with shared_lock:
        # Відкидає дублікати, якщо id меседжа вже приходила раніше або лежить в буфері
        if message_id <= len(messages) or message_id in message_buffer:
            logging.info(f"Duplicate message with message_id {message_id} ignored.")
            return jsonify({"status": "Duplicate"}), 200
    
        # Відкладає меседжі в буфер до того моменту, поки не прийдуть всі повідомлення, що йдуть перед ним
        if message_id > last_processed_id + 1:
            logging.info(f"Buffering message with message_id: {message_id}")
            message_buffer[message_id] = message
            return jsonify({"status": "Buffered"}), 200
            
        # Кладе очікуваний меседж в список і оновлює last_processed_id
        process_message(message_id, message)
        
        # Процесить відкладені меседжі з буферу, які тепер стали актуальними, якщо такі є
        while last_processed_id + 1 in message_buffer:
            buffered_seq_id = last_processed_id + 1
            buffered_message = message_buffer.pop(buffered_seq_id)
            process_message(buffered_seq_id, buffered_message)
        
    return jsonify({"status": "Replicated"}), 200

# Кладе меседж в список і оновлює last_processed_id
def process_message(seq_id, message):
    global last_processed_id
    messages.append({"message_id": seq_id, "message": message})
    last_processed_id = seq_id
    logging.info(f"Processed message with sequence_id: {seq_id}")

@app.route('/messages', methods=['GET'])
def get_messages():
    return jsonify(messages)


# Ендпойнти для полегшення тестування (виставляє затримку виконання для одного наступного запиту)
@app.route('/test/delay', methods=['POST'])
def introduce_delay_for_next_replication():
    delay_seconds = request.json.get('delay_seconds')
    if delay_seconds < 0:
        return "Delay must be zero or positive", 400
    
    test_delay_seconds_queue.put(delay_seconds)
    return "Success", 200

# Обнуляє повідомлення й затримки
@app.route('/test/reset', methods=['POST'])
def reset_servers():
    global messages, test_delay_seconds_queue, last_processed_id, message_buffer
    messages = []
    last_processed_id = 0
    message_buffer = {}
    test_delay_seconds_queue = queue.Queue()
    return "Success", 200
    
# Health checks 
@app.route('/heartbeat', methods=['GET'])
def heartbeat():
    return jsonify({"status": "alive"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
