from base import *
import time

### Перевіряє, що /append повертає 200, як тільки отримує кількість ACK, вказану у write_concern
def test_write_concern():
    print("")
    print("Iteration 2. test_write_concern start")
    reset()
    
    # secondary1 буде затримуватись 5 секунд на наступному виклику
    introduce_delay_for_next_replication(SECONDARY1_URL, 5)
    # secondary2 буде затримуватись 20 секунд на наступному виклику
    introduce_delay_for_next_replication(SECONDARY2_URL, 20)
    
    # Відправляє запит, w=2 (чекає, поки меседж збережеться на мастері і одному secondary)
    append_message(request_body={"message": "msg1", "write_concern": 2})
    
    # Відразу після отримання відповіді від мастера перевіряє, що повідомлення є на мастері й secondary1, а на secondary2 немає
    master_messages, secondary1_messages, secondary2_messages = get_messages_from_all_nodes()
    
    assert master_messages == secondary1_messages, "Master and Secondary1 messages do not match"
    # Оскільки в другого сервера затримка в 20сек, і write_concern: 2, то /append мастера має повертати успішний результат ще до того, як повідомлення запишеться на secondary2
    assert secondary2_messages == [], "Secondary2 messages should be empty" 
    
    # Чекає залишок часу (15+ секунд)
    time.sleep(16)
    # Перевіряє, що secondary2 теж має це повідомлення
    assert_messages_on_all_nodes_match()

    print("Iteration 2. test_write_concern passed: All messages are consistent across master and secondaries.")
    
# Ордерінг
def test_ordering():
    print("")
    print("Iteration 2. test_ordering start")
    reset()
    
    # secondary1 буде затримуватись 5 секунд
    introduce_delay_for_next_replication(SECONDARY1_URL, 5)
    # secondary2 буде затримуватись 20 секунд
    introduce_delay_for_next_replication(SECONDARY2_URL, 20)
    
    # Відправляє запит, w=2 (мастер і один secondary)
    append_message(request_body={"message": "msg1", "write_concern": 2})
    assert get_messages(SECONDARY2_URL) == [], "Messages on secondary2 should be empty at the moment"
    
    # Додає ще одне повідомлення, без затримки
    append_message(request_body={"message": "msg2", "write_concern": 3})

    time.sleep(16)
    assert_messages_on_all_nodes_match()
    
    print("Iteration 2. test_ordering passed")

def test_deduplication():
    print("")
    print("Iteration 2. test_deduplication start")
    reset()
    
    append_message(request_body={"message": "msg1"})
    
    # Додає таке ж повідомлення, але не через master, а напряму на secondary1/replicate
    response = requests.post(f"{SECONDARY1_URL}/replicate", json={'message': 'msg1', 'message_id': 1})
    assert response.status_code == 200, "Failed to append message to master"
    
    # Перевіряє, що на secondary1 тільки одне повідомлення
    print(get_messages(SECONDARY1_URL))
    assert get_messages(SECONDARY1_URL) == [{'message': 'msg1', 'message_id': 1}]
    print("Iteration 2. test_deduplication passed")

if __name__ == "__main__":
    test_write_concern()
    test_ordering()
    test_deduplication()
