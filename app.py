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
cors = CORS(app, origins='*')

@app.route("/")
def home():
    return jsonify({"message": "Flask is running!"})

# Top 5 Rented Films
@app.route("/topFilms")
def top_films():
    try:
        cursor = db.cursor(dictionary=True)
        query = """
            select f.film_id, f.title, c.name as category, count(r.rental_id) as rented
            from rental r
            join inventory i on r.inventory_id = i.inventory_id 
            join film f on i.film_id = f.film_id 
            join film_category fc on f.film_id = fc.film_id
            join category c on fc.category_id = c.category_id
            group by f.film_id, c.name, f.title order by rented desc
            limit 5;
        """
        cursor.execute(query)
        results = cursor.fetchall()
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)})

if __name__ == "__main__":
    app.run(debug=True)
