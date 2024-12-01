import requests

MASTER_URL = 'http://localhost:5000'
SECONDARY1_URL = 'http://localhost:5001'
SECONDARY2_URL = 'http://localhost:5002'

def append_message(message):
    print(f"Appending message {message} to master")
    response = requests.post(f"{MASTER_URL}/append", json=message)
    assert response.status_code == 200, "Failed to append message to master"

def get_messages(url: str):
    response = requests.get(f"{url}/messages")
    assert response.status_code == 200, f"Failed to get messages from {url}"
    return response.json()