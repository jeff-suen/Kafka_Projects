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
from employee import Employee_Salary, Employee_Agg
from producer import employee_topic_name1, employee_topic_name2

schema_name = "project_1"
table_name1 = "department_employee"
table_name2 = "department_employee_salary"

class SalaryConsumer(Consumer):
    #if running outside Docker (i.e. producer is NOT in the docer-compose file): host = localhost and port = 29092
    #if running inside Docker (i.e. producer IS IN the docer-compose file), host = 'kafka' or whatever name used for the kafka container, port = 9092
    def __init__(self, host: str = "localhost", port: str = "29092", group_id: str = ''):
        self.conf = {'bootstrap.servers': f'{host}:{port}',
                     'group.id': group_id,
                     'enable.auto.commit': True,
                     'auto.offset.reset': 'earliest'}
        super().__init__(self.conf)
        
        #self.consumer = Consumer(self.conf)
        self.keep_runnning = True
        self.group_id = group_id

    def consume(self, topics, processing_func):
        #implement your message processing logic here. Not necessary to follow the template. 
        try:
            self.subscribe(topics)
            while self.keep_runnning:
                msg = self.poll(timeout=1.0)

                if not msg:
                    print("No msg to consume...")
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                         (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        raise KafkaException(msg.error())
                    
                else:
                    print(f'processing from {self.group_id}')
                    processing_func(msg)
        finally:
            # Close down consumer to commit final offsets.
            self.close()

#or can put all functions in a separte file and import as a module
class ConsumingMethods:
    @staticmethod
    def add_employee(msg):
        e = Employee_Salary(**(json.loads(msg.value())))
        try:
            conn = psycopg2.connect(
                #use localhost if not run in Docker
                host="localhost",
                database="postgres",
                user="postgres",
                port = '5432',
                password="postgres")
            conn.autocommit = True
            cur = conn.cursor()
            
            insert_query1 = f"""
            INSERT INTO {schema_name}.{table_name1} (department, department_division, position_title, hire_date, salary) 
            VALUES (%s, %s, %s, %s, %s)
            """

            cur.execute(insert_query1, 
                        (
                            e.emp_dept,
                            e.dept_div,
                            e.position_title,
                            e.hire_date,
                            e.salary
                        ))
            

            cur.close()

        except Exception as err:
            print(err)

    @staticmethod
    def agg_salary(msg):
        e = Employee_Agg(**(json.loads(msg.value())))
        try:
            conn = psycopg2.connect(
                #use localhost if not run in Docker
                host="localhost",
                database="postgres",
                user="postgres",
                port = '5432',
                password="postgres")
            conn.autocommit = True
            cur = conn.cursor()
            
            insert_query2 = f"""
            INSERT INTO {schema_name}.{table_name2} (department, total_salary) 
            VALUES (%s, %s)
            """

            cur.execute(insert_query2, (e.emp_dept, e.emp_salary))
            cur.close()

        except Exception as err:
            print(err)    


if __name__ == '__main__':
    def custom_processing(msg):
        topic = msg.topic() 
        if topic == employee_topic_name1:
            ConsumingMethods.add_employee(msg)
        elif topic == employee_topic_name2:
            ConsumingMethods.agg_salary(msg)

    consumer = SalaryConsumer(group_id="employee_salary_group") 
    consumer.consume([employee_topic_name1, employee_topic_name2], custom_processing)