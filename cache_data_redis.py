import json
import redis
from datetime import datetime
from shapely import wkb
import psycopg2
import requests
import http.server
import socketserver
import threading


# PostgreSQL Connection Settings
postgres_host = '10.20.38.20'
postgres_port = '30225'
postgres_db = 'postgres'
postgres_user = 'postgres'
postgres_password = 'moCs3Z1PQsLL9wrHi9mA'

# Redis Connection Settings
redis_host = '127.0.0.1'
redis_port = '6379'
redis_db = 3 # Replace with the desired Redis database index (e.g., 1)

# GeoJSON layer
geojson_data = {
    "type": "FeatureCollection",
    "features": []
}

# Function to update GeoJSON data
def update_geojson(data_list):
    try:
        geojson_data["features"] = []  # Clear existing features

        for data in data_list:
            wkt_geometry = data.get("geom_wkt", "")
            coordinates_str = wkt_geometry.replace("POINT (", "").replace(")", "")
            coordinates = [float(coord) for coord in coordinates_str.split()]

            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": coordinates
                },
                "properties": {
                    "id": str(data["id"]),
                    "unit_id": str(data["unit_id"]),
                    "location_time": str(data["location_time"]),
                    "request_time": str(data["request_time"]),
                    "stayedtimes": str(data["stayedtimes"])
                }
            }

            geojson_data["features"].append(feature)

    except Exception as e:
        print(f"Error updating GeoJSON: {str(e)}")

# Redis list key
redis_list_key = 'fms.locations_vers2:list'

# Function to serve GeoJSON data through a local web server
def serve_geojson():
    PORT = 8000

    class GeoJSONHandler(http.server.SimpleHTTPRequestHandler):
        def do_GET(self):
            try:
                all_elements = redis_connection.lrange(redis_list_key, 0, -1)
                data_list = [json.loads(element.decode('utf-8')) for element in all_elements]

                update_geojson(data_list)

                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(geojson_data).encode())

            except Exception as e:
                print(f"Error serving GeoJSON: {str(e)}")

        def do_POST(self):
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length).decode('utf-8')
            # print(f"Received POST data: {post_data}")

            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b"POST request received successfully")

    with socketserver.TCPServer(("", PORT), GeoJSONHandler) as httpd:
        print(f"Serving GeoJSON on port {PORT}...")
        httpd.serve_forever()

# Function to trigger local web server update
def update_local_web_server(server_url, serialized_data):
    try:
        response = requests.post(server_url, data=serialized_data, headers={'Content-Type': 'application/json'})
        if response.status_code == 200:
            print("Local web server update triggered successfully.")
        else:
            print(f"Failed to trigger local web server update. Status code: {response.status_code}")
    except Exception as e:
        print(f"Error triggering local web server update: {str(e)}")

# Function to cache data from PostgreSQL to Redis
def cache_data_from_postgresql_to_redis(redis_connection, server_url):
    postgres_connection = psycopg2.connect(
        host=postgres_host,
        port=postgres_port,
        database=postgres_db,
        user=postgres_user,
        password=postgres_password
    )

    postgres_cursor = postgres_connection.cursor()

    try:
        postgres_cursor.execute("WITH NumberedRows AS (SELECT*,  ROW_NUMBER() OVER (PARTITION BY unit_id ORDER BY location_time DESC) AS rn FROM fms.locations_vers2 ) SELECT * FROM NumberedRows WHERE location_time >= CURRENT_DATE - INTERVAL '1 week';")
        result_rows = postgres_cursor.fetchall()

        redis_list_key = 'fms.locations_vers2:list'
        data_to_cache_list = []

        for row in result_rows:
            wkb_data = bytes.fromhex(row[6])
            geometry = wkb.loads(wkb_data)
            wkt_data = geometry.wkt

            data_to_cache = {
                'id': row[0],
                'unit_id': row[1],
                'location_time': row[2].strftime('%Y-%m-%d %H:%M:%S'),
                'request_time': row[3].strftime('%Y-%m-%d %H:%M:%S'),
                'y_pos': float(row[4]),
                'x_pos': float(row[5]),
                'geom_wkt': wkt_data,
                'stayedtimes': row[7]
            }

            serialized_data = json.dumps(data_to_cache)
            data_to_cache_list.append(serialized_data)

        with redis_connection.pipeline() as pipeline:
            pipeline.rpush(redis_list_key, *data_to_cache_list)
            pipeline.execute()

        update_local_web_server(server_url, json.dumps(data_to_cache_list))

    except Exception as e:
        print(f"Error caching rows. Error: {str(e)}")

    finally:
        postgres_cursor.close()
        postgres_connection.close()

if __name__ == "__main__":
    redis_connection = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db)
    server_url = 'http://localhost:8000'

    server_thread = threading.Thread(target=serve_geojson)
    server_thread.start()

    cache_data_from_postgresql_to_redis(redis_connection, server_url)
