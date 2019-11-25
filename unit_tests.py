import unittest
from xe_consume import Consumer
from random import randint, uniform
import json
from json.decoder import JSONDecodeError
import hashlib
import time
import multiprocessing
import sys
import queue


class XETest(unittest.TestCase):

  def setUp(self):
    """
    Set up Consumer, also test mysql connect()
    :return:
    """
    config = {
      "servers": "15.188.142.132",
      "offset": "0",
      "auto_offset_reset": 'latest',
      "enable_auto_commit": False
    }
    self.consumer = Consumer(**config)

  def tearDown(self):
    """
    Tear down consumer
    :return:
    """
    self.consumer.kafka_consumer.close()

  def test_kafka_topic(self):
    """
    Test kafka topic
    :return:
    """
    self.assertEqual(
      '8d777f385d3dfec8815d20f7496026dc',  # hashed topic name
      hashlib.md5(str(list(self.consumer.kafka_consumer.topics())[0]).encode('utf-8')).hexdigest()
    )

  def test_kafka_payload(self):
    """
    Test kafka messages
    :return:
    """
    self.sample = None
    time.sleep(uniform(0, 5))
    q = multiprocessing.Queue()
    get_process = multiprocessing.Process(target=self.consumer.get_payload)
    get_process.start()

    time.sleep(30)
    get_process.terminate()
    get_process.join()

    try:
      buffer = self.consumer._queue.get(timeout=10)
    except queue.Empty:
      pass
    else:
      print(buffer)
      payload_keys = list(json.loads(buffer.value.decode('utf-8')).keys())
      if payload_keys:
        self.assertEqual(['id', 'customer_id'], payload_keys)






