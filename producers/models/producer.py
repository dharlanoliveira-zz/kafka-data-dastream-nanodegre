"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        self.broker_properties = {
            'bootstrap.servers': 'localhost:9092',
            'schema.registry.url': 'http://localhost:8081',
            'on_delivery': self.delivery_report
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(
            self.broker_properties,
            default_key_schema=key_schema, default_value_schema=value_schema
         )

    def delivery_report(err, msg):
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))     

    def create_topic(self):
        logger.info(f'Creating topic {self.topic_name}')
        admin_client = KafkaAdminClient(bootstrap_servers=self.broker_properties['bootstrap.servers'], client_id=f'producer{self.topic_name}!')
        admin_client.create_topics(new_topics=[NewTopic(name=self.topic_name, num_partitions=self.num_replicas, replication_factor=self.num_replicas)], validate_only=False)
        logger.info(f'Topic {self.topic_name} created')

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        self.producer.close()
        logger.info("producer close complete")
