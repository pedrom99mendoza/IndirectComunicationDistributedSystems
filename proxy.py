import redis
import random
import time
import pika
# Conéctate a Redis

r = redis.Redis(host='localhost', port=6379, db=0)

#terminal pollution
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', port='8000'))
channel = connection.channel()
channel.queue_declare(queue='co2_queue', durable=True)

#terminal wellness
connection1 = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', port='9000'))
channel1 = connection1.channel()
channel1.queue_declare(queue='wellness_queue', durable=True)

aux = 0
aux1 = 0

# Eliminar todos los datos de todas las bases de datos en Redis
r.flushall()

while True:

    #Permite seleccionar una base de datos específica utilizando su índice.
    r.select(1)
    # Obtener todas las claves en la base de datos actual
    k=r.keys('*')
	
    l = len(k)
    while (aux < l):
    	print ("clau : ", k[aux])
    	pollution = r.get(k[aux])
    	print ("valor: ", pollution)
    	
    	print ("----------elemento pollution numero: ", aux, "----------")
    	aux = aux + 1
    	pollution_str = pollution.decode()
    	print("pollution:", pollution_str)
    	message = pollution_str
    	channel.basic_publish(exchange='', routing_key='co2_queue', body=message, properties=pika.BasicProperties( delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE ))
    	print("\n")

    #Permite seleccionar una base de datos específica utilizando su índice.
    r.select(0)
    # Obtener todas las claves en la base de datos actual
    k1=r.keys('*')
    l1=len(k1)
    while (aux1<l1):
		
    	print ("key : ", k1[aux1])
    	wellness = r.get(k1[aux1])
    	print ("value: ", wellness)
    	
    	print ("[---------elemento wellness numero: ", aux1,"---------]")
    	aux1 = aux1 + 1
    	wellness_str = wellness.decode()
    	print("wellness:", wellness_str)
    	message1 = wellness_str
    	channel1.basic_publish(exchange='', routing_key='wellness_queue', body=message1, properties=pika.BasicProperties( delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE ))
    	print("\n")
