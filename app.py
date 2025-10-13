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
                    (
                        select count(i_avail.inventory_id)
                        from inventory i_avail
                        left join rental r_active on i_avail.inventory_id = r_active.inventory_id and r_active.return_date is null
                        where i_avail.film_id = f.film_id and i_avail.store_id = 1 and r_active.rental_id is null
                    ) as copies_avail
            from film f
            join film_category fc on f.film_id = fc.film_id
            join category c on fc.category_id = c.category_id
            join film_actor fa on f.film_id = fa.film_id
            join actor a on fa.actor_id = a.actor_id
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

# Fetch Actor Details
@app.route("/actorDetails", methods=["GET"])
def actor_details():
    try:
        actor_id = request.args.get('actor_id', type=int)
        if not actor_id:
            return jsonify({"error": "Actor ID is required"}), 400

        db = get_db_connect()
        cursor = db.cursor(dictionary=True)

        # Fetch actor details
        actor_query = """
            SELECT actor_id, first_name, last_name, last_update
            FROM actor
            WHERE actor_id = %s;
        """
        cursor.execute(actor_query, (actor_id,))
        actor = cursor.fetchone()

        if not actor:
            return jsonify({"message": "Actor not found"}), 404

        # Fetch films for the actor
        films_query = """
            SELECT f.film_id, f.title, f.release_year
            FROM film_actor fa
            JOIN film f ON fa.film_id = f.film_id
            WHERE fa.actor_id = %s
            ORDER BY f.title;
        """
        cursor.execute(films_query, (actor_id,))
        films = cursor.fetchall()

        actor["films"] = films

        cursor.close()
        db.close()

        return jsonify(actor)

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
# Validate Customer based on ID
@app.route("/validateCustomer/<int:customer_id>", methods=["GET"])
def validate_customer(customer_id):
    try:
        db = get_db_connect()
        cursor = db.cursor(dictionary=True)

        query = """
            SELECT customer_id, first_name, last_name, active
            FROM customer
            WHERE customer_id = %s;
        """
        cursor.execute(query, (customer_id,))
        customer = cursor.fetchone()

        cursor.close()
        db.close()

        if customer:
            if customer["active"]:
                return jsonify({"valid": True, "message": "Customer is valid and active", "customer": customer}), 200
            else:
                return jsonify({"valid": False, "message": "Customer exists but is inactive"}), 200
        else:
            return jsonify({"valid": False, "message": "Customer ID not found"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
# Rents a Film out to Customer
@app.route("/rentFilm", methods=["POST"])
def rent_film():
    try:
        data = request.get_json()
        film_id = data.get("film_id")
        customer_id = data.get("customer_id")

        if not film_id or not customer_id:
            return jsonify({"error": "Film ID and Customer ID are required"}), 400

        db = get_db_connect()
        cursor = db.cursor(dictionary=True)

        find_available_inventory_query = """
            select i.inventory_id
            from inventory i
            left join rental r on i.inventory_id = r.inventory_id and r.return_date is null
            where i.film_id = %s and i.store_id = 1 and r.rental_id is null
            limit 1;
        """
        cursor.execute(find_available_inventory_query, (film_id,))
        available_inventory = cursor.fetchone()

        if not available_inventory:
            cursor.close()
            db.close()
            return jsonify({"error": "No available copies for this film"}), 404

        inventory_id = available_inventory["inventory_id"]

        insert_rental_query = """
            INSERT INTO rental (rental_date, inventory_id, customer_id, staff_id, last_update)
            VALUES (NOW(), %s, %s, 1, NOW());
        """
        cursor.execute(insert_rental_query, (inventory_id, customer_id))
        db.commit()

        cursor.close()
        db.close()

        return jsonify({"success": True, "message": f"Film ID {film_id} rented to Customer ID {customer_id} using Inventory ID {inventory_id}"}), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Fetch Customer Details
@app.route("/customerDetails/<int:customer_id>", methods=["GET"])
def customer_details(customer_id):
    try:
        db = get_db_connect()
        cursor = db.cursor(dictionary=True)

        query = """
            select c.customer_id, c.first_name, c.last_name, c.email, c.active, c.create_date, c.last_update,
                   a.address, a.address2, a.district, ci.city, co.country, a.postal_code, a.phone
            from customer c
            left join address a on c.address_id = a.address_id
            left join city ci on a.city_id = ci.city_id
            left join country co on ci.country_id = co.country_id
            where c.customer_id = %s;
        """
        cursor.execute(query, (customer_id,))
        customer = cursor.fetchone()

        cursor.close()
        db.close()

        if customer:
            return jsonify(customer), 200
        else:
            return jsonify({"message": "Customer not found"}), 404

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
# Customer Rental History
@app.route("/customerHistory/<int:customer_id>", methods=["GET"])
def customer_history(customer_id):
    try:
        db = get_db_connect()
        cursor = db.cursor(dictionary=True)

        query = """
            select r.rental_id, r.rental_date, r.return_date, f.film_id, f.title
            from rental r
            join inventory i on r.inventory_id = i.inventory_id
            join film f on i.film_id = f.film_id
            where r.customer_id = %s
            order by r.rental_date desc;
        """

        cursor.execute(query, (customer_id,))
        rentals = cursor.fetchall()

        current_rentals = []
        prev_rentals = []

        for rental in rentals:
            if rental["return_date"] is None:
                current_rentals.append(rental)
            else:
                prev_rentals.append(rental)

        cursor.close()
        db.close()

        return jsonify({
            "current_rentals": current_rentals,
            "previous_rentals": prev_rentals
        }), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}),500
    
# Return Film
@app.route("/returnFilm", methods=["POST"])
def return_film():
    try:
        data = request.get_json()
        rental_id = data.get("rental_id")

        if not rental_id:
            return jsonify({"error": "RentalID is required"}), 400

        db = get_db_connect()
        cursor = db.cursor(dictionary=True)

        query = """
            update rental
            set return_date = now(), last_update = NOW()
            where rental_id = %s and return_date is null;
        """

        cursor.execute(query, (rental_id,))
        db.commit()

        if cursor.rowcount == 0:
            cursor.close()
            db.close()
            return jsonify({"error": "Rental not found or already returned"}), 404

        cursor.close()
        db.close()

        return jsonify({"success": True, "message": f"Film returned for Rental ID {rental_id}"}),200

    except Exception as e:
        return jsonify({"error": str(e)}),500
    
# Update Customer Info
@app.route("/updateCustomer", methods=["PUT"])
def update_customer():
    try:
        data = request.get_json()
        customer_id = data.get("customer_id")

        if not customer_id:
            return jsonify({"error": "Customer ID is required for update"}), 400

        db = get_db_connect()
        cursor = db.cursor()

        fetch_address_id_query = "SELECT address_id FROM customer WHERE customer_id = %s;"
        cursor.execute(fetch_address_id_query, (customer_id,))
        customer_record = cursor.fetchone()

        if not customer_record:
            return jsonify({"error": "Customer not found"}), 404

        customer_address_id = customer_record[0] 

        customer_updates = []
        customer_update_values = []

        if "first_name" in data:
            customer_updates.append("first_name = %s")
            customer_update_values.append(data["first_name"])
        if "last_name" in data:
            customer_updates.append("last_name = %s")
            customer_update_values.append(data["last_name"])
        if "email" in data:
            customer_updates.append("email = %s")
            customer_update_values.append(data["email"])
        if "active" in data:
            customer_updates.append("active = %s")
            customer_update_values.append(data["active"])

        if customer_updates:
            customer_updates_sql = ", ".join(customer_updates)
            final_customer_values = tuple(customer_update_values + [customer_id])
            customer_query = f"UPDATE customer SET {customer_updates_sql}, last_update = NOW() WHERE customer_id = %s;"
            cursor.execute(customer_query, final_customer_values)

        address_updates = []
        address_update_values = []

        if "address" in data:
            address_updates.append("address = %s")
            address_update_values.append(data["address"])
        if "address2" in data:
            address_updates.append("address2 = %s")
            address_update_values.append(data["address2"])
        if "district" in data:
            address_updates.append("district = %s")
            address_update_values.append(data["district"])
        if "city_id" in data:
            address_updates.append("city_id = %s")
            address_update_values.append(data["city_id"])
        if "postal_code" in data:
            address_updates.append("postal_code = %s")
            address_update_values.append(data["postal_code"])
        if "phone" in data:
            address_updates.append("phone = %s")
            address_update_values.append(data["phone"])

        if address_updates:
            address_updates_sql = ", ".join(address_updates)
            final_address_values = tuple(address_update_values + [customer_address_id])
            address_query = f"UPDATE address SET {address_updates_sql}, last_update = NOW() WHERE address_id = %s;"
            cursor.execute(address_query, final_address_values)

        db.commit()
        cursor.close()
        db.close()

        return jsonify({"success": True, "message": f"Customer {customer_id} updated successfully."}), 200
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
# Delete Customer
@app.route("/deleteCustomer/<int:customer_id>", methods=["DELETE"])
def delete_customer(customer_id):
    try:
        db = get_db_connect()
        cursor = db.cursor()

        # Delete from customer table
        delete_customer_query = "delete from customer where customer_id = %s;"
        cursor.execute(delete_customer_query, (customer_id,))

        db.commit()
        cursor.close()
        db.close()

        return jsonify({"success": True, "message": f"Customer {customer_id} deleted successfully."}), 200

    except Exception as e:
        db.rollback()
        # if customer has rentals
        return jsonify({"error": f"Failed to delete customer: {str(e)}"}), 500

if __name__ == "__main__":
    app.run(debug=True, use_reloader=False)