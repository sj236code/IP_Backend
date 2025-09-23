from flask import Flask, jsonify, request
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

        # Top 5 Actors in Films in Stock
        query = """
            select a.actor_id, a.first_name, a.last_name, COUNT(*) AS film_count
            from inventory i
            join film f on i.film_id = f.film_id
            join film_actor fa on f.film_id = fa.film_id
            join actor a on fa.actor_id = a.actor_id
            where i.store_id = 1
            group by a.actor_id, a.first_name, a.last_name
            order by film_count desc
            limit 5;
        """

        cursor.execute(query)
        top_actors = cursor.fetchall()

        # Top 5 movies each Actor is in 
        top_film_query = """
            select f.film_id, f.title, count(r.rental_id) as rental_count
            from film_actor fa
            join film f on fa.film_id = f.film_id
            join inventory i on f.film_id = i.film_id
            join rental r on i.inventory_id = r.inventory_id
            where fa.actor_id = %s and i.store_id = 1
            group by f.film_id, f.title
            order by rental_count desc
            limit 5; 
        """

        for actor in top_actors:
            cursor.execute(top_film_query, (actor["actor_id"],))
            films = cursor.fetchall()
            actor["top_films"] = films

        cursor.close()
        db.close()
        return jsonify(top_actors)
    
    except Exception as e:
        return jsonify({"error": str(e)})
    
# Search Film by Title, Actor Name, or Category
@app.route("/searchFilm", methods=['GET'])
def search_film():
    try:
        user_search = request.args.get('query', '')
        db = get_db_connect()
        cursor = db.cursor(dictionary=True)

        query = """
            (select f.film_id as id, f.title as value, 'film' as type
            from film f
            where f.title like %s)
            union
            (select a.actor_id as id, concat(a.first_name, ' ',a.last_name) as value, 'actor' as type
            from actor a 
            where a.first_name like %s or a.last_name like %s
            )
            union
            (select c.category_id as id, c.name as value, 'category' as type
            from category c
            where c.name like %s)
        """

        wildcard = f"%{user_search}%"
        cursor.execute(query, (wildcard, wildcard, wildcard, wildcard))
        results = cursor.fetchall()

        cursor.close()
        db.close()

        return jsonify(results)
    
    except Exception as e:
        return jsonify({"error": str(e)})

if __name__ == "__main__":
    app.run(debug=True, use_reloader=False)
