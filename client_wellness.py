import pika
import sys
import time
import meteo_utils
import redis

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost',port ='5672'))
channel = connection.channel()
channel.queue_declare(queue='task_queue', durable=True)

while True:
    
    detector = meteo_utils.MeteoDataDetector()
    meteo_data_dict = detector.analyze_air()
    temperature = meteo_data_dict["temperature"]
    humidity = meteo_data_dict["humidity"]
    
    t_actual = time.time()
    timestamp = time.ctime(t_actual)
    message = str(temperature)+"/"+str(humidity)+"/"+str(timestamp)

    channel.basic_publish(exchange='',routing_key='task_queue',body=message,properties=pika.BasicProperties( delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE ))
    print(" [x] Sent %r" % message)
    time.sleep(3)

connection.close()
