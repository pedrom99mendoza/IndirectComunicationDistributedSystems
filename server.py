import pika
import time
import meteo_utils
import sys
import redis
# docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3.11-management
#docker run -it --rm --name rabbitmq2 -p 8000:5672 -p 15673:15672 rabbitmq:3.11-management
#docker run -it --rm --name rabbitmq3 -p 9000:5672 -p 15674:15672 rabbitmq:3.11-management
# docker run -it --privileged --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3.11-management
# redis-server

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', port='5672'))
channel = connection.channel()
channel.queue_declare(queue='task_queue', durable=True)
r = redis.Redis()

print(' [*] Esperant Missatge. To exit press CTRL+C')

processor = meteo_utils.MeteoDataProcessor()


class RawMeteoData:
    def __init__(self, temperature, humidity):
        self.temperature = temperature
        self.humidity = humidity
    


class RawPollutionData:
    def __init__(self, pollution):
        self.co2 = pollution


def callback(ch, method, properties, body):
    print(" [x] Received %r" % body.decode())
    aux = body.decode()
    aux1 = aux.split("/")

    if float(aux1[0])>299.0: 
        print("pollution")
        pollution_data = RawPollutionData(float(aux1[0]))
        coeficient = processor.process_pollution_data(pollution_data)
        r.select(1)
        r.set(aux1[0], coeficient) 
        

    else:
        print("wellness")
        meteo_data = RawMeteoData(float(aux1[0]),(float( aux1[1])))
        coeficient1 = processor.process_meteo_data(meteo_data)
        r.select(0)
        r.set(aux1[0], coeficient1) 
        

    
    print(' [*] Guardat al Redis')
    print(" --Done--")
    ch.basic_ack(delivery_tag=method.delivery_tag)
    print("\n")
    time.sleep(1)
    



channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='task_queue', on_message_callback=callback)
channel.start_consuming()
