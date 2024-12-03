from confluent_kafka import Producer
import json
import time
import random
from configs import kafka_config
from colorama import Fore, Style, init

# Initialize colored output
init(autoreset=True)

my_name = "IOT"
topic_name_in = f"{my_name}_iot_sensors_data"

# Callback for logging the result of message delivery
def delivery_report(err, msg):
    if err is not None:
        print(f"{Fore.RED}Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"{Fore.GREEN}Sent: {Style.BRIGHT}{msg.value().decode('utf-8')}{Style.RESET_ALL}")

# Kafka Producer configuration
producer_config = {
    'bootstrap.servers': kafka_config['bootstrap_servers'],
    'security.protocol': kafka_config['security_protocol'],
    'sasl.mechanism': kafka_config['sasl_mechanism'],
    'sasl.username': kafka_config['username'],
    'sasl.password': kafka_config['password']
}
producer = Producer(producer_config)

# Generating data in an infinite loop
try:
    while True:
        data = {
            "id": random.randint(1, 100),
            "temperature": random.uniform(-50, 50),
            "humidity": random.uniform(0, 100),
            "timestamp": time.time()
        }
        producer.produce(
            topic=topic_name_in,
            value=json.dumps(data).encode('utf-8'),
            callback=delivery_report
        )
        producer.flush()
        time.sleep(1)
except KeyboardInterrupt:
    print(f"{Fore.YELLOW}Data generation interrupted by user.")

finally:
    print(f"{Fore.RED}Kafka producer closed.")
