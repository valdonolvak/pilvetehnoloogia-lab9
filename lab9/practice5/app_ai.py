import json
from datetime import datetime
import os
from flask import Flask, render_template, request
from azure.storage.blob import BlobServiceClient
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.exceptions as exceptions
#import pika
import uuid

app = Flask(__name__)

# MUUTUJATE MÄÄRAMINE failide üleslaadimiseks
UPLOAD_FOLDER = './static/images'  # Parandatud teekide eraldajad
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Azure Blob Storage jaoks muutujate kirjeldamine
CONN_KEY = os.getenv('CONN_KEY')
storage_account = os.getenv('STORAGE_ACCOUNT')

blob_service_client = BlobServiceClient(account_url=f"https://{storage_account}.blob.core.windows.net/", credential=CONN_KEY)

#blob_service_client = BlobServiceClient(account_url="https://" + storage_account + ".blob.core.windows.net/", credential=CONN_KEY)

# Azure CosmosDB muutujad
COSMOS_URL = os.getenv('COSMOS_URL')
MasterKey = os.getenv('MasterKey')
DATABASE_ID = 'lab9messagesdb'
CONTAINER_ID = 'lab9messages'

cosmos_db_client = cosmos_client.CosmosClient(COSMOS_URL, {'masterKey': MasterKey})
cosmos_db = cosmos_db_client.get_database_client(DATABASE_ID)
container = cosmos_db.get_container_client(CONTAINER_ID)

# Brokeri autentimis- ja hostiinfo
username = os.getenv('USERNAME')
password = os.getenv('PASSWORD')
credentials = pika.PlainCredentials(username, password)

broker_host = os.getenv('BROKER_IP')
broker_port = os.getenv('BROKER_PORT')

# RabbitMQ parameetrite
rabbitmq_url = os.getenv('RABBITMQ_URL')
exchange_name = os.getenv('EXCHANGE_NAME')


def get_rabbit_connection():
    """Ühenduse loomine RabbitMQ'ga ning and declares exchange."""
    params = pika.URLParameters(rabbitmq_url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.exchange_declare(exchange=exchange_name, exchange_type='topic', durable=True)
    return channel


def publish_message(message, routing_key):
    channel = get_rabbit_connection()
    channel.basic_publish(exchange=exchange_name, routing_key=routing_key, body=message)


def insert_blob(img_path):
    print(img_path)
    filename = os.path.basename(img_path)  # Võta ainult failinimi ilma teekita
    blob_client = blob_service_client.get_blob_client(container=images_container, blob=filename)
    with open(file=img_path, mode="rb") as data:
        blob_client.upload_blob(data, overwrite=True)


def publish_rabbitmq(new_message, blob_path, messagetype):
    try:
        channel = get_rabbit_connection()
        message_to_send = {"content": new_message, "blob_path": blob_path}  # Parandatud 'message' -> 'new_message'

        routing_key = 'messageboard.messages.urgent' if messagetype == 'urgent' else 'messageboard.messages.noturgent'
        channel.basic_publish(
            exchange=exchange_name,
            routing_key=routing_key,
            body=json.dumps(message_to_send)
        )
    except Exception as e:
        print(f"Failed to publish message: {e}")
    finally:
        channel.close()


def insert_cosmos(img_path, content):
    new_message = {
        'content': content,
        'img_path': img_path,
        'timestamp': datetime.now().isoformat(" ", "seconds"),
        'id': str(uuid.uuid4())
    }
    try:
        container.create_item(body=new_message)
    except exceptions.CosmosResourceExistsError:
        print("Resource already exists, didn't insert message.")


def read_cosmos():
    """ Read all messages from cosmos db """
    messages = list(container.read_all_items(max_item_count=10))
    return messages


# The Flask route, defining the main behavior of the webserver:
@app.route("/handle_message", methods=['POST'])
def handleMessage():
    new_message = request.form['msg']
    img_path = ""

    if 'file' in request.files and request.files['file']:
        image = request.files['file']
        img_path = os.path.join(UPLOAD_FOLDER, image.filename)
        image.save(img_path)
        insert_blob(img_path)
        blob_path = 'https://' + storage_account + '.blob.core.windows.net/' + images_container + '/' + image.filename
        if new_message:
            insert_cosmos(blob_path, new_message)
        else:
            new_message = "__ no msg available here __"
            insert_cosmos(blob_path, new_message)

    return render_template('handle_message.html', message=new_message)


# The Flask route, defining the main behavior of the webserver:
@app.route("/", methods=['GET'])
def htmlForm():
    data = read_cosmos()
    return render_template('home.html', messages=data)


if __name__ == "__main__":
    app.run(debug=True)  # Lisa debug=True, et saaksid vigu hõlpsamini jälgida
