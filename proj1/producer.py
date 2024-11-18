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

import csv
import json
import os


from confluent_kafka import Producer
from employee import Employee_Agg, Employee_Salary
import confluent_kafka
import pandas as pd
from confluent_kafka.serialization import StringSerializer


employee_topic_name1 = "employee_salary"
employee_topic_name2 = "department_agg_salary"
csv_file = 'Employee_Salaries.csv'

#Can use the confluent_kafka.Producer class directly
class salaryProducer(Producer):
    #if connect without using a docker: host = localhost and port = 29092
    #if connect within a docker container, host = 'kafka' or whatever name used for the kafka container, port = 9092
    def __init__(self, host="localhost", port="29092"):
        self.host = host
        self.port = port
        producerConfig = {'bootstrap.servers':f"{self.host}:{self.port}",
                          'acks' : 'all'}
        super().__init__(producerConfig)
     

class DataHandler:
    '''
    Your data handling logic goes here. 
    You can also implement the same logic elsewhere. Your call
    '''
    def __init__(self, csv_file):
        self.csv_file = csv_file
    
    def extract_transform(self):
        # Extracting
        emp_pd = pd.read_csv(self.csv_file)
        # Handling Missing value
        pd_cleaned = emp_pd.fillna(0)
        # Filtering Departments
        dep_emp = pd_cleaned[
            (pd_cleaned['Department'] == 'ECC') | 
            (pd_cleaned['Department'] == 'CIT') | 
            (pd_cleaned['Department'] == 'EMS')
            ]
        # Round off the Salary to lower number
        dep_emp.loc[:, 'Salary'] = dep_emp['Salary'] // 1
        # Filtering: Employees hired after 2010
        filtered_emp = dep_emp[pd.to_datetime(dep_emp['Initial Hire Date']).dt.year >= 2010]
        # Aggregate salary by department
        agg_dept = filtered_emp.groupby('Department')['Salary'].sum().reset_index()
        # Reorder columns for each df
        filtered_emp_order = filtered_emp[['Department', 'Department-Division', 'Position Title', 'Initial Hire Date', 'Salary']]
        agg_dept_order = agg_dept[['Department', 'Salary']]

        return filtered_emp_order, agg_dept_order

if __name__ == '__main__':
    encoder = StringSerializer('utf-8')
    reader = DataHandler(csv_file)
    producer = salaryProducer()
    filtered_emp, agg_dept = reader.extract_transform()
    
    for line in filtered_emp.itertuples(index=False):
        emp1 = Employee_Salary.from_csv_line(line)
        producer.produce(employee_topic_name1, key=encoder(emp1.emp_dept), value=encoder(emp1.to_json()))
        producer.poll(1)
    
    for line in agg_dept.itertuples(index=False):
        emp2 = Employee_Agg.from_csv_line(line)
        producer.produce(employee_topic_name2, key=encoder(emp2.emp_dept), value=encoder(emp2.to_json()))
        producer.poll(1)

    '''
    # implement other instances as needed
    # you can let producer process line by line, and stop after all lines are processed, or you can keep the producer running.
    # finish code with your own logic and reasoning

    for line in lines:
        emp = Employee.from_csv_line(line)
        producer.produce(employee_topic_name, key=encoder(emp.emp_dept), value=encoder(emp.to_json()))
        producer.poll(1)
    '''
    