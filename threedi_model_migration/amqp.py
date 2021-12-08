import logging
import pika
from .hg import decode


logger = logging.getLogger(__name__)


def consume(url, queue, func):
    """Call func(slug=message) for each message received via AMQP"""
    def callback(ch, method, properties, body):
        """To be used as on_message callback with channel.basic_consume
        
        Expects repository slugs as plain bytes. Send them with:
        
        $ amqp-publish -r my-queue -b my-repo-slug
        """
        logger.info(f"Received {body}")
        try:
            func(slug=decode(body))
        except Exception:
            logger.exception(f"Error processing {body}")

    # https://pika.readthedocs.io/en/stable/examples/blocking_consume_recover_multiple_hosts.html
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=url))
            try:
                channel = connection.channel()
                channel.queue_declare(queue=queue)
                channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=True)
                logger.info(f"Listening to queue '{queue}'...")
                channel.start_consuming()
            except KeyboardInterrupt:
                channel.stop_consuming()
                connection.close()
                break
        except pika.exceptions.AMQPConnectionError:
            # Reconnect on all connection errors
            continue
