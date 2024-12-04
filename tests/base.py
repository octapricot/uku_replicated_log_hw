import requests

MASTER_URL = 'http://localhost:5000'
SECONDARY1_URL = 'http://localhost:5001'
SECONDARY2_URL = 'http://localhost:5002'
SECONDARIES = [SECONDARY1_URL, SECONDARY2_URL]

def append_message(request_body):
    print(f"Appending message {request_body} to master")
    response = requests.post(f"{MASTER_URL}/append", json=request_body)
    assert response.status_code == 200, "Failed to append message to master"

def get_messages(url: str):
    response = requests.get(f"{url}/messages")
    assert response.status_code == 200, f"Failed to get messages from {url}"
    return response.json()

def introduce_delay_for_next_replication(url: str, delay_seconds: int):
    response = requests.post(f"{url}/test/delay", json={"delay_seconds": delay_seconds})
    assert response.status_code == 200, "Failed to set up the delay"
    
# Скидання всіх меседжів і затримок на нодах для тестування
def reset():
    response = requests.post(f"{MASTER_URL}/test/reset")
    assert response.status_code == 200, "Failed to reset"
    
def get_messages_from_all_nodes():
    master_messages = get_messages(MASTER_URL)
    print(f"Master messages: {master_messages}")

    secondary1_messages = get_messages(SECONDARY1_URL)
    print(f"Secondary1 messages: {secondary1_messages}")

    secondary2_messages = get_messages(SECONDARY2_URL)
    print(f"Secondary2 messages: {secondary2_messages}")
    return (master_messages, secondary1_messages, secondary2_messages)

def assert_messages_on_all_nodes_match():
    master_messages, secondary1_messages, secondary2_messages = get_messages_from_all_nodes()
    assert master_messages == secondary1_messages, "Master and Secondary1 messages do not match"
    assert master_messages == secondary2_messages, "Master and Secondary2 messages do not match"