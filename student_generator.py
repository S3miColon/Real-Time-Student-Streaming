from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

names = ['Aditya', 'Rahul', 'Aman', 'Priya', 'Neha', 'Rohit']
subjects = ['Python', 'Java', 'Big Data', 'SQL', 'Spark']

print("Automatic Data Generator Started...")

i = 1

while True:
    student = {
        'student_id': i,
        'student_name': random.choice(names),
        'subject': random.choice(subjects),
        'marks': random.randint(30, 100),
        'event_time': datetime.now().isoformat()
    }

    producer.send('student_data', student)

    print("Data Sent:", student)

    i += 1
    time.sleep(2)
