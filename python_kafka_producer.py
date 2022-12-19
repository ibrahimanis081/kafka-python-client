
from kafka import KafkaProducer
import pickle
import time


producer = KafkaProducer(bootstrap_servers="localhost:9092",
                        key_serializer=lambda k: pickle.dumps(k),
                        value_serializer=lambda v: pickle.dumps(v))
                        
for number in range(100):
    producer.send(topic="python_test", key=number, value="Hello from Python")
    print(f"message {number} sent to kafka")
    time.sleep(2)

producer.flush()
producer.close()
print("Producer finished")

    
