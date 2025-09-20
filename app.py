from flask import Flask, jsonify
from flask_cors import CORS
import mysql.connector

app = Flask(__name__)
CORS(app, origins='*')

def get_db_connect():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="Flash-11",
        database="sakila"
    )

@app.route("/")
def home():
    return jsonify({"message": "Flask is running."})

# Top 5 Rented Films
@app.route("/topFilms", methods=['GET'])
def top_films():
    try:
        db = get_db_connect()
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

        cursor.close()
        db.close()

        return jsonify(results)
    
    except Exception as e:
        return jsonify({"error": str(e)})
    
# Top 5 Actors that are part of films that I have in store
@app.route("/topActors", methods=['GET'])
def top_actors():
    try:
        db = get_db_connect()
        cursor = db.cursor(dictionary=True)

        query = """
            SELECT a.actor_id, a.first_name, a.last_name, COUNT(fa.film_id) AS movies
            FROM actor a
            JOIN film_actor fa ON a.actor_id = fa.actor_id
            JOIN film f ON fa.film_id = f.film_id
            JOIN inventory i ON f.film_id = i.film_id
            WHERE i.store_id = 1
            GROUP BY a.actor_id, a.first_name, a.last_name
            ORDER BY movies DESC
            LIMIT 5;
        """

        cursor.execute(query)
        results = cursor.fetchall()

        cursor.close()
        db.close()
        return jsonify(results)
    
    except Exception as e:
        return jsonify({"error": str(e)})

if __name__ == "__main__":
    app.run(debug=True, use_reloader=False)
