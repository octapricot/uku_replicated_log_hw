from base import *

def test_replication():
    
    # 1. Append a message to the master
    append_message(message={"message": "msg1"})
    
    # 2. Retrieve messages from master
    master_messages = get_messages(MASTER_URL)
    print(f"Master messages: {master_messages}")
    
    # 3. Retrieve messages from secondary1
    secondary1_messages = get_messages(SECONDARY1_URL)
    print(f"Secondary1 messages: {secondary1_messages}")
    
    # 4. Retrieve messages from secondary2
    secondary2_messages = get_messages(SECONDARY2_URL)
    print(f"Secondary2 messages: {secondary2_messages}")
    
    # 5. Verify that the master's messages match the secondaries' messages
    assert master_messages == secondary1_messages, "Master and Secondary1 messages do not match"
    assert master_messages == secondary2_messages, "Master and Secondary2 messages do not match"
    
    print("Test passed: All messages are consistent across master and secondaries.")

if __name__ == "__main__":
    test_replication()