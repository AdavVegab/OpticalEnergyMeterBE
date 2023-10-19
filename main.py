import paho.mqtt.client as mqtt
import mysql.connector
from mysql.connector import Error
from datetime import datetime

# MQTT broker address and port number
broker_address = "test.mosquitto.org"
broker_port = 1883

# MQTT topic
mqtt_topic = "ESP-01_01/Pulses"

# MySQL server address and port number
mysql_host = "localhost"
mysql_port = 3306

# MySQL credentials
mysql_user = "root"
mysql_password = "opticalenergymeter"

# MySQL database and table name
database_name = "oem"
table_name = "sensordata"


def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    client.subscribe(mqtt_topic)

def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))
    try:
        # Connect to MySQL server
        connection = mysql.connector.connect(host=mysql_host,
                                             port=mysql_port,
                                             user=mysql_user,
                                             password=mysql_password)
        cursor = connection.cursor()

        # Check if database exists
        cursor.execute("SHOW DATABASES LIKE %s", (database_name,))
        result = cursor.fetchone()
        if not result:
            # Create database if it does not exist
            cursor.execute("CREATE DATABASE "+database_name)

        # Check if table exists
        cursor.execute("USE "+database_name)
        cursor.execute("SHOW TABLES LIKE %s", (table_name,))
        result = cursor.fetchone()
        if not result:
            # Create table if it does not exist
            cursor.execute("CREATE TABLE "+table_name+" (id INT AUTO_INCREMENT PRIMARY KEY, message VARCHAR(255), timestamp TIMESTAMP)")

        # Insert message into table with current timestamp
        sql_query = "INSERT INTO "+table_name+" (message, timestamp) VALUES (%s, %s)"
        cursor.execute(sql_query, (msg.payload, datetime.now()))
        connection.commit()

    except Error as e:
        print("Error while connecting to MySQL", e)

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(broker_address, broker_port, 60)

client.loop_forever()