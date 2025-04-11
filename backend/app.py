from flask import Flask, request, jsonify
from flask_cors import CORS
from pymongo import MongoClient

app = Flask(__name__)
CORS(app)

# Connect to MongoDB StatefulSet
client = MongoClient("mongodb://mongo:27017/")
db = client["testdb"]
collection = db["items"]

@app.route("/insert", methods=["POST"])
def insert():
    data = request.json
    if not data or "name" not in data:
        return jsonify({"error": "Invalid input"}), 400
    item_id = collection.insert_one({"name": data["name"]}).inserted_id
    return jsonify({"message": "Inserted", "id": str(item_id)}), 201

@app.route("/delete/<string:name>", methods=["DELETE"])
def delete(name):
    result = collection.delete_one({"name": name})
    if result.deleted_count == 0:
        return jsonify({"error": "Not found"}), 404
    return jsonify({"message": "Deleted"}), 200

@app.route("/list", methods=["GET"])
def list_items():
    items = list(collection.find({}, {"_id": 0, "name": 1}))
    return jsonify(items), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
