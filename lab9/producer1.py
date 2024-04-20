import pika
import random
import datetime
import json
import threading

broker_host = "172.17.66.26"
broker_port = "5672"

#Autentimisinfo
username = "admin"
password = "Passw0rd!"
credentials = pika.PlainCredentials (username, password)

connection = pika.BlockingConnection (
        pika.ConnectionParameters (host = broker_host, port = broker_port, credentials = credentials))
channel = connection.channel()


message = {
    "id": "f634f37d-218d-4f04-a5ab-8e61b14ceedd",
    "content": "Parcel is arriving in 10min",
    "img_path": "https://mywebsite.blob.core.windows.net/lab9/fognode.png",
    "timestamp": "2024-04-08 09:20:57"
}

message_str = json.dumps(message)

routing_key = "messageboard.messages.urgent"

exchange = "Nolvak"

#channel.basic_publish (
#        exchange = exchange,
#        routing_key = routing_key,
#        body = message_str)



# Funktsioon sõnumi edastamiseks
def send_message():
    channel.basic_publish(
        exchange=exchange,
        routing_key=routing_key,
        body=message_str)

# Funktsioon sõnumite edastamise tsükli käivitamiseks
def send_messages():
    while True:
        send_message()
        # Oota 5 sekundit enne järgmise sõnumi saatmist
        threading.Event().wait(5)

# Käivita sõnumite edastamise tsükkel uues lõimes
send_thread = threading.Thread(target=send_messages)
send_thread.start()

# Oota kasutaja klaviatuuri katkestust
input("Press any key to stop sending messages...\n")

# Katkesta sõnumite saatmine
send_thread.join()

# Sulge ühendus
connection.close()
