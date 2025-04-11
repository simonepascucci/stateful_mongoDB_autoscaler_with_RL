from kubernetes import client, config
import time
import pymongo

# Load Kubernetes config (Minikube)
config.load_incluster_config()

# Kubernetes API client
v1_apps = client.AppsV1Api()

# Namespace where MongoDB StatefulSet is deployed
NAMESPACE = "default"
STATEFULSET_NAME = "mongo"
MONGO_SERVICE_NAME = "mongo"  # Service name used by MongoDB pods

ADDRESS = "mongo-0.mongo.default.svc.cluster.local"
PORT = 27017
WAITING_TIME = 60

# Establish connection to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongo:27017/")
admin_db = mongo_client.admin

print(mongo_client.list_database_names())


def get_replica_count():
    """Fetches the current number of replicas for the StatefulSet."""
    statefulset = v1_apps.read_namespaced_stateful_set(STATEFULSET_NAME, NAMESPACE)
    return statefulset.spec.replicas


def scale_statefulset(new_replicas):
    """Scales the StatefulSet to the specified number of replicas."""
    patch_body = {"spec": {"replicas": new_replicas}}
    v1_apps.patch_namespaced_stateful_set_scale(
        name=STATEFULSET_NAME, namespace=NAMESPACE, body=patch_body
    )
    print(f"Scaled {STATEFULSET_NAME} to {new_replicas} replicas.")


def get_mongo_replica_count():
    """Fetches the number of members in the MongoDB replica set."""
    try:
        response = admin_db.command("replSetGetStatus")
        members = response.get("members", [])
        return len(members)
    except Exception as e:
        print(f"Error retrieving replica set status: {e}")
        return -1


def add_replica_to_mongo(new_replica):
    """Adds a new replica to the MongoDB replica set."""
    try:
        response = admin_db.command("replSetReconfig", {
            "_id": "rs0",
            "version": 1,
            "members": [
                {"_id": i, "host": member["host"]} for i, member in enumerate(admin_db.command("replSetGetStatus")["members"])
            ] + [{"_id": get_mongo_replica_count(), "host": new_replica}]
        }, force=True)
        print(f"Added {new_replica} to the MongoDB replica set.")
        return response
    except Exception as e:
        print(f"Error adding replica: {e}")
        return None


def remove_replica_from_mongo(replica):
    """Removes a replica from the MongoDB replica set."""
    try:
        current_members = admin_db.command("replSetGetStatus")["members"]
        filtered_members = [m for m in current_members if m["host"] != replica]
        response = admin_db.command("replSetReconfig", {
            "_id": "rs0",
            "version": 1,
            "members": [
                {"_id": i, "host": member["host"]} for i, member in enumerate(filtered_members)
            ]
        }, force=True)
        print(f"Removed {replica} from the MongoDB replica set.")
        return response
    except Exception as e:
        print(f"Error removing replica: {e}")
        return None


def auto_scale():
    """Automatically scales up and down the StatefulSet while updating MongoDB replica set."""
    current_k8s_replicas = get_replica_count()
    print(f"Starting with: {current_k8s_replicas} replicas")

    current_mongodb_replicas = get_mongo_replica_count()
    print(f"Starting with: {current_mongodb_replicas} MongoDB replicas")

    if current_mongodb_replicas != current_k8s_replicas:
        print("Mismatch between Kubernetes and MongoDB replicas. Adjusting...")
    else:
        print("Kubernetes and MongoDB replicas are in sync.")
        while True:
            print(f"Current replicas: {current_k8s_replicas}")

            # Scale up
            new_replicas = current_k8s_replicas + 1
            scale_statefulset(new_replicas)

            # Add new replica to MongoDB replica set
            new_pod_name = f"mongo-{new_replicas - 1}"
            new_replica_address = f"{new_pod_name}.mongo.default.svc.cluster.local:{PORT}"
            add_replica_to_mongo(new_replica_address)

            current_k8s_replicas = get_replica_count()
            print(f"Current replicas: {current_k8s_replicas}")

            time.sleep(WAITING_TIME)

            # Scale down
            last_pod_name = f"mongo-{new_replicas - 1}"
            last_replica_address = f"{last_pod_name}.mongo.default.svc.cluster.local:{PORT}"
            remove_replica_from_mongo(last_replica_address)

            # Ensure at least 1 replica remains
            final_replicas = max(1, new_replicas - 1)
            scale_statefulset(final_replicas)

            current_k8s_replicas = get_replica_count()
            print(f"Current replicas: {current_k8s_replicas}")

            time.sleep(WAITING_TIME)  # Wait before next iteration


if __name__ == "__main__":
    auto_scale()
