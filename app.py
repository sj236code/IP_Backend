from flask import Flask, jsonify
from flask_cors import CORS
import mysql.connector

db= mysql.connector.connect(
    host="localhost",
    user="root",
    password="Flash-11",
    database="sakila"
)

app = Flask(__name__)
CORS(app)

@app.route("/")
def home():
    return jsonify({"message": "Flask is running!"})


@app.route("/testdb")
def test_db():
    try:
        cursor = db.cursor(dictionary=True)
        cursor.execute("SELECT * FROM actor LIMIT 5;")
        results = cursor.fetchall()
        return jsonify(results)   # return first 5 rows from actor table
    except Exception as e:
        return jsonify({"error": str(e)})

if __name__ == "__main__":
    app.run(debug=True)
