
import pickle
from kafka import KafkaConsumer


consumer = KafkaConsumer("python_test", 
                         auto_offset_reset="latest",
                         key_deserializer=lambda k: pickle.loads(k),
                         value_deserializer=lambda v: pickle.loads(v))
                         
for message in consumer:
    if message is None:
        print("Waiting for message")
    else:
        print(f"message recieved: key:{message.key}, value:{message.value} in partition {message.partition}")