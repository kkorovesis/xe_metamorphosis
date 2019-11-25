import os
import sys
import random
from kafka import KafkaConsumer
from kafka.structs import TopicPartition
from utils import *
import queue
import multiprocessing
from json.decoder import JSONDecodeError
import datetime
import logging
dt = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
filename = '.log/log_{}'.format(dt)
logging.basicConfig(filename=filename, level=logging.DEBUG)
logger = logging.getLogger()


class Consumer:
  """
  My Kafka Consumer for XE

    Keyword Arguments:
        servers: Kafka servers (KafkaConsumer.bootstrap_servers).
        topic: Kafka topic to be consumed.
        offset: Set offset for manually seeking spesific offset in partion
        auto_offset_reset: Offset reset strategy (KafkaConsumer.auto_offset_reset).
        enable_auto_commit: Periodically commit last offset (KafkaConsumer.enable_auto_commit).
        consumer_timeout_ms: Time out (ms) before raising StopIteration (KafkaConsumer.consumer_timeout_ms).
    
  """
  def __init__(self, **kwargs):
    self.servers = kwargs.get('servers', "15.188.142.132")
    self.topics = kwargs.get('topics', 'data')
    self.offset = kwargs.get('offset', None)
    self.auto_offset_reset = kwargs.get('auto_offset_reset', 'earliest')
    self.enable_auto_commit = kwargs.get('enable_auto_commit', True)
    self.consumer_timeout_ms = kwargs.get('consumer_timeout_ms', 500)
    self.db = None
    self.cursor = None
    self.sample = None
    self._queue = multiprocessing.Queue()
    self.db, self.cursor = db_connect(
      host=os.environ.get('XE_MYSQL_HOST'),
      db_name=os.environ.get('XE_MYSQL_DBNAME'),
      user=os.environ.get('XE_MYSQL_USER'),
      password=os.environ.get('XE_MYSQL_PASSWORD'),
    )
    logging.basicConfig(
      format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
      level=logging.INFO
    )
    logging.getLogger('kafka').setLevel(logging.INFO)
    self.kafka_consumer = KafkaConsumer(bootstrap_servers=self.servers,
                                        auto_offset_reset=self.auto_offset_reset,
                                        enable_auto_commit=self.enable_auto_commit,
                                        consumer_timeout_ms=self.consumer_timeout_ms,
                                        )
    self.mypartition = TopicPartition(self.topics, 0)
    self.assigned_topic = [self.mypartition]
    self.kafka_consumer.assign(self.assigned_topic)

  def get_payload(self):
    """
    Get message payload from stream
    :return:
    """
    while True:
      time.sleep(random.uniform(0, 5))
      for message in self.kafka_consumer:
        try:
          self._queue.put(json.loads(message.value.decode('utf-8')))
        except JSONDecodeError:
          pass

  def get_messages(self):
    """
    Get full messages from stream. Used to save last offset and payload for manual setting offset
    :param q:
    :return:
    """
    while True:
      time.sleep(random.uniform(0, 5))
      for message in self.kafka_consumer:
        try:
          value = json.loads(message.value.decode('utf-8'))
          self._queue.put({
            "offset": message.offset,
            "value": json.dumps(value)
          })
        except JSONDecodeError:
          pass

  def process_payload(self):
    """
    Process message payload
    :return:
    """
    messages = []
    start = time.time()
    while True:
      try:
        buffer_data = self._queue.get(timeout=5)
      except queue.Empty:
        pass
      else:
        messages.append(buffer_data)
      end = time.time()
      if end - start > 5:
        if messages:
          inserted = self.db_insert_payload(messages, debug=False)
          start = end
          messages = []
          # print(f"{inserted} messages inserted")

  def db_insert_payload(self, products, debug):
    """
    Insert to
    :param products:
    :param debug:
    :return:
    """

    inserted = 0
    for item in products:

      if item.get('id', None) and not self.cursor.execute(f'''SELECT id from Classifieds WHERE id = '{item['id']}' '''):
        data = {
          "id": item['id'],
          "customerId": item['customer_id'],
          "createdAt": item['created_at'],
          "text": item.get('text', None),  # this contains <p></p> tags
          "adType": item.get('ad_type', None),
          "price": item.get('price', None),
          "currency": item.get('currency', None),
          "paymentType": item.get('payment_type', None),
          "paymentCost": item.get('payment_cost', None),
        }
        insert_params = {
          'table': 'Classifieds',
          'data': data,
          'insert': False,
        }
        if debug:  # not working now
          insert_params.update({
            'insert': False,
            'print_q': True
          })
        q, vals = db_insert_sql(**insert_params)
        inserted += self.cursor.execute(q, vals)
        self.db.commit()
    return inserted


if __name__ == "__main__":
  consumer = None
  try:
    config = {
      "servers": "15.188.142.132",
      "auto_offset_reset": 'latest',
      "topics": "data"
    }
    consumer = Consumer(**config)
    get_process = multiprocessing.Process(target=consumer.get_payload)
    get_process.start()

    insert_process = multiprocessing.Process(target=consumer.process_payload)
    insert_process.start()

    get_process.join()
    insert_process.join()

  except KeyboardInterrupt:
    print('Interrupted')
    consumer.kafka_consumer.close()
    try:
      sys.exit(0)
    except SystemExit:
      os._exit(0)






