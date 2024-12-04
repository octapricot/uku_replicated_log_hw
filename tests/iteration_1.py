from base import *

def test_replication():
    print("Iteration 1. test_replication start")
    reset()
    
    # secondary1 буде затримуватись 5 секунд
    introduce_delay_for_next_replication(SECONDARY1_URL, 2)
    # secondary2 буде затримуватись 20 секунд
    introduce_delay_for_next_replication(SECONDARY2_URL, 2)
    
    append_message(request_body={"message": "msg1"})
    
    assert_messages_on_all_nodes_match()

    print("Test passed: All messages are consistent across master and secondaries.")

if __name__ == "__main__":
    test_replication()