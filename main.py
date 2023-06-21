"""
Storing relations between users in graph db and storing user attributes in the mongodb

USER - (id, FIRST_NAME, LAST_NAME)
User1 (ash2shukla, Ashish, Shukla)
User2 (message_aj, Ayush, Jaiswal)
User3 (sypherv, Ravi, Goyal)
User4 (tusharsr, Tushar, Srivastava)

ash2shukla follows message_aj
message_aj follows ash2shukla, tusharsr
sypherv follows None
tusharsr follows ash2shukla, message_aj, sypherv
"""

from db import gremlin_client, mongo_client, setup_mongo_db_collection, setup_gremlin_graph
from loguru import logger
from enum import Enum

class Relation(Enum):
    FOLLOWS = "follows"

# create user
def create_new_user(user: dict):
    """Create new user record in mongo and vertex in graph"""
    result = mongo_collection.update_one({"id": user["id"]}, {"$set": user}, upsert=True)
    logger.info(f"upsert - id {user['id']} - modified {result.modified_count}")
    if result.upserted_id:
        query = f"""
        g.addV('user')
        .property('id', '{user["id"]}')
        .property('pk', 'pk')
        """
        callback = gremlin_client.submitAsync(query)
        if callback.result() is not None:
            logger.info(f"Inserted this vertex:\n\t{callback.result().all().result()}")
        else:
            logger.debug(f"Something went wrong with this query: {query}")


# follow request
def add_follower(follower_id: str, following_id: str):
    """Create a follows relationship between two users"""
    query = f"""
    g.V('{follower_id}')
    .addE('{Relation.FOLLOWS.value}')
    .to(g.V('{following_id}'))
    """
    callback = gremlin_client.submitAsync(query)
    if callback.result() is not None:
        logger.info(f"Inserted this edge:\n\t{callback.result().all().result()}")
    else:
        logger.debug(f"Something went wrong with this query: {query}")

def get_user_followers(user_id):
    """IDs of all the users that follow the current user"""
    # edge out from other users, in to current user
    query = f"""
    g.V('{user_id}')
    .in('{Relation.FOLLOWS.value}')
    .hasLabel('user')
    .values('id')
    """
    callback = gremlin_client.submitAsync(query)
    return list(callback.result())
    
def get_user_following(user_id):
    """IDs of all the users that the current user follows"""
    # edge out from current user, in to other users
    query = f"""
    g.V('{user_id}')
    .out('{Relation.FOLLOWS.value}')
    .hasLabel('user')
    .values('id')
    """
    callback = gremlin_client.submitAsync(query)
    return list(callback.result())

def get_all_users():
    """Get all users from mongo"""
    all_users = []
    result = mongo_collection.find({}, {"_id": 0, "id": 1, "first_name": 1, "last_name": 1})
    for user in result:
        user.update({
            "followers": get_user_followers(user["id"]),
            "following": get_user_following(user["id"]),
        })
        all_users.append(user)
    return all_users

USERS = [
    {"id": "ash2shukla", "first_name": "Ashish", "last_name": "Shukla"},
    {"id": "message_aj", "first_name": "Ayush", "last_name": "Jaiswal"},
    {"id": "sypherv", "first_name": "Ravi", "last_name": "Goel"},
    {"id": "tusharsr", "first_name": "Tushar", "last_name": "Srivastava"},
    {"id": "dhulam", "first_name": "Ajit", "last_name": "Dhulam"},
]

FOLLOWER_DATA = {
    # follower : following ...
    "ash2shukla": ("message_aj",),
    "message_aj": ("ash2shukla", "tusharsr"),
    "sypherv": None,
    "tusharsr": ("ash2shukla", "message_aj", "sypherv"),
}
 
MONGO_DB_NAME = "xca"
MONGO_COLLECTION_NAME = "users"
mongo_collection = setup_mongo_db_collection(mongo_client, MONGO_DB_NAME, MONGO_COLLECTION_NAME)
setup_gremlin_graph(gremlin_client)

def main():
    # create all users
    for user in USERS:
        create_new_user(user)

    # map followers
    for follower_id, following in FOLLOWER_DATA.items():
        if following is None:
            logger.info(f"{follower_id} follows no one")
            continue
        for target_id in following:
            add_follower(follower_id, target_id)

    # get all users' data
    # user attributes, followers, following
    all_users = get_all_users()
    logger.info("All Users' Data")
    logger.info(all_users)

if __name__ == "__main__":
    main()