"""
Copyright (C) 2024 BeaconFire Staffing Solutions
Author: Ray Wang

This file is part of Oct DE Batch Kafka Project 1 Assignment.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""


import json
import random
import string
import sys
import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.serialization import StringDeserializer
from employee import Employee
from producer import employee_topic_name

schema_name = "project_2"
table_name = "employees"

class cdcConsumer(Consumer):
    #if running outside Docker (i.e. producer is NOT in the docer-compose file): host = localhost and port = 29092
    #if running inside Docker (i.e. producer IS IN the docer-compose file), host = 'kafka' or whatever name used for the kafka container, port = 9092
    def __init__(self, host: str = "localhost", port: str = "29092", group_id: str = ''):
        self.conf = {'bootstrap.servers': f'{host}:{port}',
                     'group.id': group_id,
                     'enable.auto.commit': True,
                     'auto.offset.reset': 'earliest'}
        super().__init__(self.conf)
        self.keep_runnning = True
        self.group_id = group_id

    def consume(self, topics, processing_func):
        try:
            self.subscribe(topics)
            while self.keep_runnning:
                #implement your logic here
                msg = self.poll(timeout=1.0)
                if msg is None:
                    print("No msg to consume...")
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                         (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    print(f'processing from {self.group_id} with the action: {processing_func(msg)}')
        finally:
            # Close down consumer to commit final offsets.
            self.close()

def update_dst(msg):
    e = Employee(**(json.loads(msg.value())))
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            port = '5433', # change this port number to align with the docker compose file
            password="postgres")
        conn.autocommit = True
        cur = conn.cursor()
        #your logic goes here
        # Retriving the action data from messages
        action = e.action

        if action.lower() == "insert":
            # Insert the record into employee table in the destination database
            cur.execute(
                f"""
                INSERT INTO {schema_name}.{table_name} (emp_id, first_name, last_name, dob, city)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (e.emp_id, e.first_name, e.last_name, e.dob, e.city),
            )
            print(f"Inserted record: {e.emp_id, e.first_name, e.last_name, e.dob, e.city}")
        
        elif action.lower() == "update":
            # Update the record in employee table in the destination database
            cur.execute(
                f"""
                UPDATE {schema_name}.{table_name} 
                SET first_name = %s, last_name = %s, dob = %s, city = %s
                WHERE emp_id = %s
                """,
                (e.first_name, e.last_name, e.dob, e.city, e.emp_id),
            )
            print(f"Updated record: {e.emp_id, e.first_name, e.last_name, e.dob, e.city}")
        
        elif action.lower() == "delete":
            # Delete the record from employee table in the destination database
            cur.execute(
                f"""
                DELETE FROM {schema_name}.{table_name} 
                WHERE emp_id = %s
                """,
                (e.emp_id,),
            )
            print(f"Deleted record: {e.emp_id, e.first_name, e.last_name, e.dob, e.city}")
            
        cur.close()

        return action
    except Exception as err:
        print(err)

if __name__ == '__main__':
    consumer = cdcConsumer(group_id= "employee_project_2") 
    consumer.consume([employee_topic_name], update_dst)
