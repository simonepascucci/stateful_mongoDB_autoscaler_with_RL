from kubernetes import client, config
import time
import subprocess
from pymongo import MongoClient
from bson import json_util

# Load Kubernetes config (Minikube)
config.load_kube_config()

# Kubernetes API client
v1_apps = client.AppsV1Api()
v1_core = client.CoreV1Api()

# Namespace where MongoDB StatefulSet is deployed
NAMESPACE = "default"
STATEFULSET_NAME = "mongo"
MONGO_SERVICE_NAME = "mongo"  # Service name used by MongoDB pods

ADDRESS = ".mongo.default.svc.cluster.local"
PORT = 27017

WAITING_TIME = 60

def get_replica_count():
    """Fetches the current number of replicas for the StatefulSet."""
    statefulset = v1_apps.read_namespaced_stateful_set(STATEFULSET_NAME, NAMESPACE)
    return statefulset.spec.replicas

def scale_statefulset(new_replicas):
    """Scales the StatefulSet to the specified number of replicas."""
    patch_body = {"spec": {"replicas": new_replicas}}
    v1_apps.patch_namespaced_stateful_set_scale(
        name=STATEFULSET_NAME,
        namespace=NAMESPACE,
        body=patch_body
    )
    print(f"Scaled {STATEFULSET_NAME} to {new_replicas} replicas.")

def exec_mongo_command(pod_name, command):
    """Executes a MongoDB command inside a given pod using kubectl."""
    full_command = f"kubectl exec -it {pod_name} -n {NAMESPACE} -- mongosh --eval '{command}'"
    result = subprocess.run(full_command, shell=True, capture_output=True, text=True)
    result = subprocess.check_output(full_command, shell=True)
    return result.decode('utf-8')

def get_mongo_replica_count():
    """Fetches the number of members in the MongoDB replica set."""
    #response = exec_mongo_command("mongo-0", "rs.status()")
    full_command = f"kubectl exec -it mongo-0 -n {NAMESPACE} -- mongosh --eval 'rs.status()' --json=canonical"
    response = subprocess.check_output(full_command, shell=True)
    try:
        data = json_util.loads(response)
        members = data.get("members", [])
        return len(members)
    except Exception as e:
        print(f"Error decoding EJSON: {e}")
        return -1 


def add_replica_to_mongo(new_replica):
    """Adds a new replica to the MongoDB replica set."""
    command = f'rs.add("{new_replica}")'
    res = exec_mongo_command("mongo-0", command)
    print(f"Added {new_replica} to the MongoDB replica set.")
    return res

def remove_replica_from_mongo(replica):
    """Removes a replica from the MongoDB replica set."""
    command = f'rs.remove("{replica}")'
    res = exec_mongo_command("mongo-0", command)
    print(f"Removed {replica} from the MongoDB replica set.")
    return res

def auto_scale():
    """Automatically scales up and down the StatefulSet while updating MongoDB replica set."""

    current_k8s_replicas = get_replica_count()
    print(f"Starting with: {current_k8s_replicas} replicas")
    
    print("Kubernetes and MongoDB replicas are in sync.")
    
    while True:
        print(f"Current replicas: {current_k8s_replicas}")

        # Scale up
        new_replicas = current_k8s_replicas + 1
        scale_statefulset(new_replicas)

        # Add new replica to MongoDB replica set
        new_pod_name = f"mongo-{new_replicas - 1}"
        print("new_pod_name:", new_pod_name)
        new_replica_address = f"{new_pod_name}{ADDRESS}:{PORT}"
        print("new_replica:", new_replica_address)
        add_replica_to_mongo(new_replica_address)

        current_k8s_replicas = get_replica_count()
        print(f"Current replicas: {current_k8s_replicas}")

        time.sleep(WAITING_TIME)

        # Scale down
        last_pod_name = f"mongo-{new_replicas - 1}"
        print("last_pod_name:", last_pod_name)
        last_replica_address = f"{last_pod_name}{ADDRESS}:{PORT}"
        print("last_replica:", last_replica_address)
        remove_replica_from_mongo(last_replica_address)

        # Ensure at least 3 replicas remain
        final_replicas = max(3, new_replicas - 1)
        scale_statefulset(final_replicas)

        current_k8s_replicas = get_replica_count()
        print(f"Current replicas: {current_k8s_replicas}")

        time.sleep(WAITING_TIME)  # Wait before next iteration

if __name__ == "__main__":
    auto_scale()
