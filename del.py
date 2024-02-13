import redis

# Redis Connection Settings
redis_host = '127.0.0.1'
redis_port = '6379'
redis_db = 3 # Specify the desired Redis database index

# Connect to Redis
redis_connection = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db)

# Use FLUSHDB to delete all data from the specified Redis database
redis_connection.flushdb()

print(f"All data deleted from Redis database {redis_db}.")
