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

# Retrieve Film Details Given a Title
@app.route("/filmDetails", methods=["GET"])
def film_details():
    try:
        film_title = request.args.get('title', '')
        # print("Searching for ", film_title)
        db = get_db_connect()
        cursor = db.cursor(dictionary=True)

        query = """
            select f.film_id, f.title, f.description, f.release_year, f.language_id,
                    f.rental_duration, f.rental_rate, f.length, f.replacement_cost, 
                    f.rating, cast(f.special_features AS CHAR) as special_features,
                    c.name as category, 
                    group_concat(distinct concat(a.first_name, ' ', a.last_name) separator ', ') as actors, 
                    count(distinct i.inventory_id) as copies_avail
            from film f
            join film_category fc on f.film_id = fc.film_id
            join category c on fc.category_id = c.category_id
            join film_actor fa on f.film_id = fa.film_id
            join actor a on fa.actor_id = a.actor_id
            left join inventory i on f.film_id = i.film_id and i.store_id = 1
            where f.title like %s
            group by f.film_id, f.title, f.description, f.release_year, f.language_id,
                    f.rental_duration, f.rental_rate, f.length, f.replacement_cost, 
                    f.rating, f.special_features, c.name;
        """

        wildcard = f"%{film_title}%"
        cursor.execute(query, (wildcard,))
        results = cursor.fetchall()

        cursor.close()
        db.close()

        if not results:
            return jsonify({"message" : "Film not found"}), 404
        
        print(results)

        return jsonify(results)
    
    except Exception as e:
        return jsonify({"error": str(e)})

# Search Customers Query
@app.route("/searchCustomer", methods=["GET"])
def search_customer():
    try:
        query = request.args.get('query', '')
        db = get_db_connect()
        cursor = db.cursor(dictionary=True)

        # Default return all Customers
        if not query or query.strip() == "":
            sql = """
                select c.customer_id, c.first_name, c.last_name, c.email, c.active, 
                concat(a.address, coalesce(concat(' ', a.address2), '')) as address,
                a.district, ci.city, a.phone
                from customer c
                left join address a on c.address_id = a.address_id
                left join city ci on a.city_id = ci.city_id
                order by c.customer_id
            """
            cursor.execute(sql)
        # Search by Customer id, First Name, or Last Name
        else:
            sql = """
                select c.customer_id, c.first_name, c.last_name, c.email, c.active, 
                concat(a.address, coalesce(concat(' ', a.address2), '')) as address,
                a.district, ci.city, a.phone
                from customer c
                left join address a on c.address_id = a.address_id
                left join city ci on a.city_id = ci.city_id
                where c.customer_id like %s
                    or c.first_name like %s
                    or c.last_name like %s
                order by c.customer_id
            """
            wildcard = f"%{query}%"
            cursor.execute(sql, (wildcard, wildcard, wildcard))
        
        results = cursor.fetchall()

        cursor.close()
        db.close()

        print(f"Found {len(results)} customers")
        return jsonify(results)
    
    except Exception as e:
        return jsonify({"error": str(e)})

# Add New Customer to DB
@app.route("/addCustomer", methods=["POST"])
def add_customer():
    try:
        data = request.get_json()

        customer_fields = ['first_name', 'last_name', 'address', 'district']
        for field in customer_fields:
            if field not in data:
                return jsonify({'error': f'Missing required field: {field}'}), 400
            
        db = get_db_connect()
        cursor = db.cursor()

        address_query = """
            insert into address (address, address2, district, city_id, postal_code, phone, location, last_update)
            values (%s, %s, %s, %s, %s, %s, ST_GeomFromText('POINT(0 0)'), now())   
        """

        address_values = (
            data['address'],
            data.get('address2', ''),
            data['district'], 1, 
            data.get('postal_code', ''), 
            data.get('phone', ''),
        )

        cursor.execute(address_query, address_values)
        address_id = cursor.lastrowid
        
        customer_query = """
            insert into customer (store_id, first_name, last_name, email, address_id, active, create_date, last_update)
            values (%s, %s, %s, %s, %s, %s, now(), now())
        """

        customer_values = (
            1, data['first_name'],
            data['last_name'],
            data.get('email', ''),
            address_id, 1
        )

        cursor.execute(customer_query, customer_values)
        customer_id = cursor.lastrowid

        db.commit()
        cursor.close()
        db.close()
        
        return jsonify({
            'added': True,
            'message': 'Customer Added',
            'customer_id' : customer_id
        }), 201
    
    except Exception as e:
        return jsonify({"error": str(e)})

if __name__ == "__main__":
    app.run(debug=True, use_reloader=False)
