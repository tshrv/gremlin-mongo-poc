import os

import pymongo
from dotenv import load_dotenv
from gremlin_python.driver import client as gremlin_python_client
from gremlin_python.driver import serializer

load_dotenv()
COSMOS_MONGO_CONNECTION_STRING = os.environ.get("COSMOS_MONGO_CONNECTION_STRING")
GREMLIN_ENDPOINT = os.environ.get("GREMLIN_ENDPOINT")
GREMLIN_USERNAME = os.environ.get("GREMLIN_USERNAME")
GREMLIN_PRIMARY_KEY = os.environ.get("GREMLIN_PRIMARY_KEY")

mongo_client = pymongo.MongoClient(COSMOS_MONGO_CONNECTION_STRING)

gremlin_client = gremlin_python_client.Client(
    GREMLIN_ENDPOINT, 'g',
    username=GREMLIN_USERNAME,
    password=GREMLIN_PRIMARY_KEY,
    message_serializer=serializer.GraphSONSerializersV2d0()
)

def setup_gremlin_graph(client):
    """Cleanup gremlin graph, drop everything"""
    _gremlin_cleanup_graph = "g.V().drop()"
    callback = client.submitAsync(_gremlin_cleanup_graph)
    if callback.result() is not None:
        callback.result().all().result() 

def setup_mongo_db_collection(client, db_name, collection_name):
    """Create fresh database and collection, clean up if already exist."""
    db = client[db_name]
    if db_name in client.list_database_names():
        db.command("dropDatabase")

    db.command({"customAction": "CreateDatabase"})
    print("Created db '{}'.\n".format(db_name))

    collection = db[collection_name]
    db.command(
        {"customAction": "CreateCollection", "collection": collection_name}
    )
    print("Created collection '{}'.\n".format(collection_name))

    return collection