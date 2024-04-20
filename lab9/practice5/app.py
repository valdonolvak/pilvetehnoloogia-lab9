import json
from datetime import datetime
import os
from flask import Flask, render_template, request
from azure.storage.blob import BlobServiceClient
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.exceptions as exceptions
import uuid
import pika



app = Flask(__name__)

UPLOAD_FOLDER ='.\static\images'

CONN_KEY= os.getenv('CONN_KEY')
storage_account = os.getenv('STORAGE_ACCOUNT')

images_conatiner = "images"

#Azure Blog Storage jaoks  muutujate kirjeldamine
CONN_KEY= os.getenv('CONN_KEY')
storage_account = os.getenv('STORAGE_ACCOUNT')
images_container = "images"

blob_service_client = BlobServiceClient(account_url=f"https://{storage_account}.blob.core.windows.net/", credential=CONN_KEY)


#Azure CosmosDB muutujad
COSMOS_URL= os.getenv('COSMOS_URL')
MasterKey= os.getenv('MasterKey')
DATABASE_ID='lab9messagesdb'
CONTAINER_ID='lab9messages'


cosmos_db_client = cosmos_client.CosmosClient(COSMOS_URL, {'masterKey': MasterKey} )
cosmos_db = cosmos_db_client.get_database_client(DATABASE_ID)
container = cosmos_db.get_container_client(CONTAINER_ID)



#Brokeri autentimis- ja hostiinfo
username = os.getenv('USERNAME')
password = os.getenv('PASSWORD')
credentials = pika.PlainCredentials (username, password)

broker_host = os.getenv('BROKER_IP')
broker_port = os.getenv('BROKER_PORT')


#RabbitMQ parameetrite
rabbitmq_url = os.getenv('RABBITMQ_URL')
exhange_name = os.getenv('EXCHANGE_NAME')


"""
#blob_service_client = BlobServiceClient(account_url="https://"+storage_account+".blob.core.windows.net/",credential=CONN_KEY)

# COSMOS_URL= os.getenv('APPSETTING_COSMOS_URL')
# MasterKey= os.getenv('APPSETTING_MasterKey')

#COSMOS_URL= os.getenv('COSMOS_URL')
#MasterKey= os.getenv('MasterKey')

#DATABASE_ID='lab9messagesdb'
#CONTAINER_ID='lab9messages'
#cosmos_db_client = cosmos_client.CosmosClient(COSMOS_URL, {'masterKey': MasterKey} )
#cosmos_db = cosmos_db_client.get_database_client(DATABASE_ID)
#container = cosmos_db.get_container_client(CONTAINER_ID)
"""

#### Pika  plain credentials võetud harjutus 2-st  ####

# Autentimisinfo ja Pika ühenduse loomine
username = "admin"
password = "Passw0rd!"
credentials = pika.PlainCredentials(username, password)

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=broker_host, port=broker_port, credentials=credentials))
channel = connection.channel()

"""
def publish_rabbitmq(new_message,blob_path,messagetype):
    channel = connection.channel()
    exchange=nolvak

    message_to_send = {"content": message,"blob_path": blob_path}
"""

#ChatGPT poolt genereeritud kood

def publish_rabbitmq(new_message, blob_path, messagetype):
    # Loome ühenduse RabbitMQ serveriga
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    # Vahetuse defineerimine
    exchange = "nolvak"
    channel.exchange_declare(exchange=exchange, exchange_type='direct')

    # Sõnumi andmestruktuuri loomine
    message_to_send = {
        "content": new_message,
        "blob_path": blob_path
    }

    message_str = json.dumps(message_to_send)

    # Marsruutvõtme määramine vastavalt sõnumi tüübile
    if messagetype == "urgent":
        routing_key = "messageboard.messages.urgent"
    else:
        routing_key = "messageboard.messages.noturgent"

    # Sõnumi avaldamine RabbitMQ-le
    channel.basic_publish(exchange=exchange, routing_key=routing_key, body=message_str)

    # Ühenduse sulgemine
    connection.close()



def insert_blob(img_path):
    print(img_path)
    filename = (img_path).split('\\')[-1]
    print(filename)
    blob_client = blob_service_client.get_blob_client(container=images_container, blob=filename)
    with open(file=img_path, mode="rb") as data:
        blob_client.upload_blob(data,overwrite=True)

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



def read_messages_from_file():
    """ Read all messages from a JSON file"""
    with open('data.json') as messages_file:
        return json.load(messages_file)

def append_message_to_file(img_path, content):
    """ Read the contents of JSON file, add this message to it's contents, then write it back to disk. """
    data = read_messages_from_file()
    

    new_message = {
        'content': content,
        'img_path': img_path,
        'timestamp': datetime.now().isoformat(" ", "seconds")
    }

    data['messages'].append(new_message)

    with open('data.json', mode='w') as messages_file:
        json.dump(data, messages_file)



# The Flask route, defining the main behaviour of the webserver:
@app.route("/handle_message", methods=['POST'])
def handleMessage():
    new_message = request.form['msg']
    messagetype = request.form['message-type']
    img_path = ""

    
    if('file' in request.files and request.files['file']):
        image = request.files['file']
        img_path = os.path.join(UPLOAD_FOLDER, image.filename)
        image.save(img_path)
        insert_blob(img_path)
        blob_path = 'https://'+storage_account+'.blob.core.windows.net/'+images_container+'/'+image.filename 
        if new_message:
            # append_message_to_file(blob_path, new_message)
            insert_cosmos(blob_path, new_message)
            publish_rabbitmq(new_message,blob_path,messagetype)
        else:
            new_message="__ no msg available here __"
            # append_message_to_file(blob_path, new_message)
            insert_cosmos(blob_path, new_message)
            publish_rabbitmq(new_message,blob_path,messagetype)

    return render_template('handle_message.html', message=new_message)



# The Flask route, defining the main behaviour of the webserver:
@app.route("/", methods=['GET'])
def htmlForm():

    # data = read_messages_from_file()
    data = read_cosmos()


    # Return a Jinja HTML template, passing the messages as an argument to the template:
    # return render_template('home.html', messages=data['messages'])
    return render_template('home.html', messages=data)
