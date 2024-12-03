from confluent_kafka.admin import AdminClient, NewTopic
from configs import kafka_config
from colorama import Fore, Style, init

# Initialize colored logging
init(autoreset=True)

# Create Kafka client
try:
    print(f"{Fore.CYAN}Connecting to Kafka Admin Client...")
    admin_client = AdminClient({
        'bootstrap.servers': kafka_config['bootstrap_servers'],
        'security.protocol': kafka_config['security_protocol'],
        'sasl.mechanism': kafka_config['sasl_mechanism'],
        'sasl.username': kafka_config['username'],
        'sasl.password': kafka_config['password']
    })
    print(f"{Fore.GREEN}Connected to Kafka Admin Client successfully.")
except Exception as e:
    print(f"{Fore.RED}Failed to connect to Kafka Admin Client: {e}")
    exit(1)

# Define topic names considering my_name
my_name = "IOT"
topic_name_in = f"{my_name}_iot_sensors_data"
alerts_topic_name = f"{my_name}_iot_alerts"
num_partitions = 2
replication_factor = 1

# Try to delete existing topics
try:
    print(f"{Fore.CYAN}Attempting to delete existing topics...")

    # Delete topics if they exist
    admin_client.delete_topics([topic_name_in, alerts_topic_name], operation_timeout=30)

    # Wait for the topic deletion process to complete
    print(f"{Fore.GREEN}Old topics deleted successfully.")
except Exception as e:
    print(f"{Fore.YELLOW}Failed to delete existing topics: {e}")

# Create topics again
topics = [
    NewTopic(topic_name_in, num_partitions, replication_factor),
    NewTopic(alerts_topic_name, num_partitions, replication_factor)
]

try:
    print(f"{Fore.CYAN}Creating topics: {topic_name_in} and {alerts_topic_name}...")

    fs = admin_client.create_topics(topics)

    for topic, future in fs.items():
        try:
            future.result()
            print(f"{Fore.GREEN}Topic '{topic}' created successfully.")
        except Exception as e:
            print(f"{Fore.YELLOW}Topic '{topic}' already exists or failed to create: {e}")

    print(f"{Fore.GREEN}Kafka topic management completed successfully.")

except Exception as e:
    print(f"{Fore.RED}An error occurred while creating topics: {e}")


# from confluent_kafka.admin import AdminClient, NewTopic
# from configs import kafka_config
# from colorama import Fore, Style, init
#
# # Initialize colored logging
# init(autoreset=True)
#
# # Create Kafka client
# try:
#     print(f"{Fore.CYAN}Connecting to Kafka Admin Client...")
#     admin_client = AdminClient({
#         'bootstrap.servers': kafka_config['bootstrap_servers'],  # Без [0]
#         'security.protocol': kafka_config['security_protocol'],
#         'sasl.mechanism': kafka_config['sasl_mechanism'],
#         'sasl.username': kafka_config['username'],
#         'sasl.password': kafka_config['password']
#     })
#     print(f"{Fore.GREEN}Connected to Kafka Admin Client successfully.")
# except Exception as e:
#     print(f"{Fore.RED}Failed to connect to Kafka Admin Client: {e}")
#     exit(1)
#
# # Define topic names considering my_name
# my_name = "IOT"
# topic_name_in = f"{my_name}_iot_sensors_data"
# alerts_topic_name = f"{my_name}_iot_alerts"
# num_partitions = 2
# replication_factor = 1
#
# # Create topics
# topics = [
#     NewTopic(topic_name_in, num_partitions, replication_factor),
#     NewTopic(alerts_topic_name, num_partitions, replication_factor)
# ]
#
# try:
#     print(f"{Fore.CYAN}Creating topics: {topic_name_in} and {alerts_topic_name}...")
#
#     fs = admin_client.create_topics(topics)
#
#     for topic, future in fs.items():
#         try:
#             future.result()
#             print(f"{Fore.GREEN}Topic '{topic}' created successfully.")
#         except Exception as e:
#             print(f"{Fore.YELLOW}Topic '{topic}' already exists or failed to create: {e}")
#
#     print(f"{Fore.GREEN}Kafka topic management completed successfully.")
#
# except Exception as e:
#     print(f"{Fore.RED}An error occurred while creating topics: {e}")