import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', port='9000'))
channel = connection.channel()
channel.queue_declare(queue='wellness_queue', durable=True)

def callback(ch, method, properties, body):
    wellness = body.decode()
    print("Wellness:", wellness)


print("Terminal wellness en curs...")

channel.basic_consume(queue='wellness_queue', on_message_callback=callback, auto_ack=True)
channel.start_consuming()
