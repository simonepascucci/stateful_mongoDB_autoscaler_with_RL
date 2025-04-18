import random
import time
from kubernetes import client, config
from pymongo import MongoClient

# Load Kubernetes config (Minikube)
config.load_kube_config()

# Kubernetes API client
v1_apps = client.AppsV1Api()
v1_core = client.CoreV1Api()

STATEFULSET_NAME = "mongo"
NAMESPACE = "default"

# Establish connection to MongoDB
mongo_client = MongoClient("mongodb://localhost:27017/", directConnection=True)
admin_db = mongo_client.admin
while admin_db.command("hello")["isWritablePrimary"] == False:
    print("Waiting for connection to MongoDB primary...")
    mongo_client = MongoClient("mongodb://localhost:27017/", directConnection=True)
    admin_db = mongo_client.admin
    time.sleep(1)



# Define global variables
current_k8s_replicas = 0
current_mongodb_replicas = 0

DNS_ADDRESS = ".mongo.default.svc.cluster.local"
PORT = 27017

SCALING_INTERVAL = 10 

def get_replica_count():
    """Fetches the current number of replicas for the StatefulSet."""
    global current_k8s_replicas
    statefulset = v1_apps.read_namespaced_stateful_set(STATEFULSET_NAME, NAMESPACE)
    current_k8s_replicas = int(statefulset.spec.replicas)

    return current_k8s_replicas

def get_mongo_replica_count():
    """Fetches the number of members in the MongoDB replica set."""
    global current_mongodb_replicas
    try:
        response = admin_db.command("replSetGetStatus")
        members = response.get("members", [])
        current_mongodb_replicas = len(members)
        return current_mongodb_replicas
    except Exception as e:
        print(f"Error retrieving replica set status: {e}")
        return -1

def scale_statefulset(new_replicas):
    """Scales the StatefulSet to the specified number of replicas."""
    global current_k8s_replicas
    patch_body = {"spec": {"replicas": new_replicas}}
    v1_apps.patch_namespaced_stateful_set_scale(
        name=STATEFULSET_NAME, namespace=NAMESPACE, body=patch_body
    )
    current_k8s_replicas = new_replicas
    print(f"Scaled {STATEFULSET_NAME} to {new_replicas} replicas.")

def add_replica_to_mongo(new_replicas_count):
    """Adds a new replica to the MongoDB replica set."""
    global current_mongodb_replicas
    new_member_config = {
        "_id": current_mongodb_replicas,
        "host": f"mongo-{new_replicas_count - 1}{DNS_ADDRESS}:{PORT}",
    }
    print(f"Adding replica with _id: {new_member_config["_id"]}")
    current_config = admin_db.command("replSetGetConfig")["config"]
    try:
        current_config["members"].append(new_member_config)
        current_config["version"] += 1
        reconfig_result = admin_db.command("replSetReconfig", current_config)
        current_mongodb_replicas += 1
        print(f"Added new replica with _id: {new_member_config["_id"]}\n")
    except Exception as e:
        print(f"Error adding new replica: {e}")

def remove_last_replica_from_mongo():
    """Removes the last replica from the MongoDB replica set."""
    global current_mongodb_replicas
    try:
        current_config = admin_db.command("replSetGetConfig")["config"]
        members = current_config["members"]

        if len(members) <= 3:
            print("Cannot remove the last replica. Minimum 3 replicas required.")
            return

        # Get the _id of the last member
        last_member_id = members[-1]["_id"]
        last_member_host = members[-1]["host"]

        print(f"Attempting to remove member with _id: {last_member_id} and host: {last_member_host}")

        # Create a new configuration excluding the last member
        new_members = [member for member in members if member["_id"] != last_member_id]
        current_config["members"] = new_members
        current_config["version"] += 1

        reconfig_result = admin_db.command("replSetReconfig", current_config)
        current_mongodb_replicas -= 1
        print(f"Removed member with _id: {last_member_id}\n")
    except Exception as e:
        print(f"Error removing last replica: {e}")

def scale_up(new_replicas_count):
    print(f"\nScaling up to: {new_replicas_count} replicas...\n")

    scale_statefulset(new_replicas_count)
    add_replica_to_mongo(new_replicas_count)
    return

def scale_down(new_replicas_count):
    print(f"\nScaling down to: {new_replicas_count} replicas...\n")

    if new_replicas_count < 3:
        print("Cannot scale down to less than 3 replicas.\n")
        return
    else:
        scale_statefulset(new_replicas_count)
        remove_last_replica_from_mongo()
        return


def auto_scale():
    global current_k8s_replicas
    global current_mongodb_replicas

    current_k8s_replicas = get_replica_count()
    print(f"Starting with: {current_k8s_replicas} Kubernetes replicas")

    current_mongodb_replicas = get_mongo_replica_count()
    print(f"Starting with: {current_mongodb_replicas} MongoDB replicas")
    

    while (current_k8s_replicas != current_mongodb_replicas):
        print("Mismatch between Kubernetes and MongoDB replicas. Adjusting...")
        if (current_k8s_replicas > current_mongodb_replicas):
            print("Scaling down statefulset...")
            difference = current_k8s_replicas - current_mongodb_replicas
            new_k8s_replicas = current_k8s_replicas - difference
            scale_statefulset(new_k8s_replicas) if new_k8s_replicas >= 3 else scale_statefulset(3)
        elif (current_mongodb_replicas > current_k8s_replicas):
            remove_last_replica_from_mongo()
            print("Removing last replica from MongoDB...")

    else:
        print("Kubernetes and MongoDB replica counts are matching.")
    
    print("Starting auto-scaling process (press q to quit)...\n")

    while True:

        random_int = 0 #random.randint(0, 1)
        print(f"\nRandom integer generated: {random_int}")

        if random_int == 0:
            scale_down(current_k8s_replicas - 1)
        elif random_int == 1:
            scale_up(current_k8s_replicas + 1)

        time.sleep(SCALING_INTERVAL)

    return


if __name__ == "__main__":

    auto_scale()

    mongo_client.close()