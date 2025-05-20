from kafka import KafkaProducer, KafkaConsumer
import json
import logging

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = ['kafka:9092']
DEFAULT_TOPIC = 'trading-signals'

def send_trading_signal(symbol, signal_type, probability, timestamp, data=None):
   try:
       producer = KafkaProducer(
           bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
           value_serializer=lambda v: json.dumps(v).encode('utf-8')
       )
       
       message = {
           'symbol': symbol,
           'signal_type': signal_type,
           'probability': probability,
           'timestamp': timestamp,
           'data': data or {}
       }
       
       future = producer.send(DEFAULT_TOPIC, message)
       producer.flush()
       future.get(timeout=10)  # Wait for the future to complete
       
       logger.info(f"Sent trading signal for {symbol} to Kafka")
       return True
   except Exception as e:
       logger.error(f"Error sending trading signal to Kafka: {str(e)}")
       return False

def consume_trading_signals(callback, topics=None, group_id='trading-signal-consumer'):
   try:
       if topics is None:
           topics = [DEFAULT_TOPIC]
           
       consumer = KafkaConsumer(
           *topics,
           bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
           group_id=group_id,
           value_deserializer=lambda v: json.loads(v.decode('utf-8')),
           auto_offset_reset='latest'
       )
       
       logger.info(f"Starting Kafka consumer for topics: {topics}")
       
       for message in consumer:
           try:
               callback(message.value)
           except Exception as e:
               logger.error(f"Error processing Kafka message: {str(e)}")
       
       return True
   except Exception as e:
       logger.error(f"Error setting up Kafka consumer: {str(e)}")
       return False
