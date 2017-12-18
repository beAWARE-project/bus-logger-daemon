from confluent_kafka import Consumer
import asyncio
import sqlite3
from datetime import datetime
import pymysql.cursors
import json


class BusConsumer:
    def __init__(self):

        # Pre-shared credentials
        self.credentials = json.load(open('bus_credentials.json'))

        # Construct required configuration
        self.configuration = {
            'client.id': 'bus_logger',
            'group.id': 'bus_logger_group',
            'bootstrap.servers': ','.join(self.credentials['kafka_brokers_sasl']),
            'security.protocol': 'SASL_SSL',
            'ssl.ca.location': '/etc/ssl/certs',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': self.credentials['api_key'][0:16],
            'sasl.password': self.credentials['api_key'][16:48],
            'api.version.request': True
        }

        self.consumer = Consumer(self.configuration)

        self.listening = True

        self.database = 'log.sqlite'

    def listen(self, topics):
        # Topics should be a list of topic names e.g. ['topic1', 'topic2']

        self.listening = True

        # Subscribe to topics
        try:
            self.consumer.subscribe(topics)
        except Exception as e:
            print(e)
            return False

        # Initiate a loop for continuous listening
        while self.listening:
            msg = self.consumer.poll(1)

            # If a message is received and it is not an error message
            if msg is not None and msg.error() is None:

                # Add incoming message to requests database
                try:
                    topic = msg.topic()
                except:
                    topic = "Undefined"

                try:
                    offset = str(msg.offset())
                except:
                    offset = "Undefined"

                try:
                    message_text = msg.value().decode('utf-8')
                except:
                    message_text = msg.value()

                # self.submit_message_to_sqlite_database(topic, message_text, offset)
                self.submit_message_to_mysql_database(topic, message_text, offset)

            # Sleep for a while
            asyncio.sleep(0.43)

        # Unsubscribe and close consumer
        self.consumer.unsubscribe()
        self.consumer.close()

    def stop(self):
        self.listening = False

    def submit_message_to_sqlite_database(self, topic, message, offset):

        # Get UTC time as string
        timestamp = datetime.utcnow().strftime("%Y/%m/%d %H:%M:%S.%f")

        try:
            con = sqlite3.connect(self.database)

            with con:
                cur = con.cursor()
                cur.execute('INSERT INTO requests (topic, message, timestamp, offset) VALUES (?, ?, ?, ?)', (topic, message, timestamp, offset))

            cur.close()

            # print('# Message logged:' + timestamp + " - Topic: " + topic)

        except sqlite3.Error as e:
            print("Error %s:" % e.args[0])
            return False

        # con = sqlite3.connect(self.database)
        #
        # with con:
        #     cur = con.cursor()
        #     cur.execute('INSERT INTO requests (message) VALUES (?)', (message,))
        #
        # cur.close()

    def submit_message_to_mysql_database(self, topic, message, offset):

        # Get UTC time as string
        timestamp = datetime.utcnow().strftime("%Y/%m/%d %H:%M:%S:%f")

        # Connect to the database
        connection = pymysql.connect(host='localhost',
                                     user='beaware',
                                     password='logger1!',
                                     db='bus_log',
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cur:

                cur.execute('INSERT INTO messages (topic, message, timestamp, offset) VALUES (%s, %s, %s, %s)', (topic, message, timestamp, offset))

            # connection is not autocommit by default. So you must commit to save
            # your changes.
            connection.commit()

            print('# Message logged:' + timestamp + " - Topic: " + topic)

        finally:
            connection.close()

    def empty_sqlite_database(self):
        # Connect to the database
        connection = pymysql.connect(host='localhost',
                                     user='beaware',
                                     password='logger1!',
                                     db='bus_log',
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cur:

                cur.execute('DELETE FROM messages')

            # connection is not autocommit by default. So you must commit to save
            # your changes.
            connection.commit()

        finally:
            connection.close()

    def empty_mysql_database(self):
        try:
            con = sqlite3.connect(self.database)

            with con:
                cur = con.cursor()
                cur.execute('DELETE FROM messages')

            cur.close()

            print("Database was cleared")

        except sqlite3.Error as e:
            print("Error %s:" % e.args[0])
            return False


