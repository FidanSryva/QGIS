

# import http.server
# import socketserver
import json
from qgis.core import QgsVectorLayer, QgsProject, QgsApplication
from PyQt5.QtCore import QTimer
import redis

# Connect to Redis
redis_connection = redis.StrictRedis(host='127.0.0.1', port=6379, db=1)


# Check if the script is running in QGIS environment
if QgsApplication.instance() is None:
    raise RuntimeError("This script must be run within the QGIS environment.")

# Function to update QGIS layer
def update_qgis_layer():
    try:
        # Add or update the GeoJSON layer in QGIS
        layer_name = 'RealTimeLayer444111'
        uri = 'http://localhost:8000'  # Assuming your local server is running on port 8000

        # Check if the layer already exists, if yes, remove it
        existing_layer = QgsProject.instance().mapLayersByName(layer_name)
        if existing_layer:
            QgsProject.instance().removeMapLayer(existing_layer[0])

        # Add the new layer to the map with an explicitly set CRS (modify as needed)
        layer = QgsVectorLayer(uri, layer_name, 'ogr')
        if not layer.isValid():
            print(f"Layer {layer_name} is not valid.")
            return

        layer.setCrs(QgsProject.instance().crs())  # Set CRS explicitly by default it is 4326
        QgsProject.instance().addMapLayer(layer)
    except Exception as e:
        print(f"Error updating QGIS layer: {str(e)}")

# Function to handle real-time updates
def handle_realtime_update():
    try:
        update_qgis_layer()
    except Exception as e:
        print(f"Error handling real-time update: {str(e)}")

# Set up a timer to periodically update the QGIS layer
update_interval_ms = 600000  # Set the update interval in milliseconds
timer = QTimer()
timer.timeout.connect(handle_realtime_update)
timer.start(update_interval_ms)

# Initial update to populate QGIS layer with existing data
handle_realtime_update()