import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', port='8000'))
channel = connection.channel()
channel.queue_declare(queue='co2_queue', durable=True)

def callback(ch, method, properties, body):
    pollution = body.decode()
    print("Pollution:", pollution)


print("Terminal pollution en curs...")

channel.basic_consume(queue='co2_queue', on_message_callback=callback, auto_ack=True)
channel.start_consuming()