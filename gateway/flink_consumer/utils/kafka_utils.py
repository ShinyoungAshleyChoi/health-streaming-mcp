"""Utility functions for Kafka operations and testing"""

import logging
from typing import Dict, List, Optional

from confluent_kafka import Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

logger = logging.getLogger(__name__)


class KafkaConnectionTester:
    """
    Utility class for testing Kafka connectivity and configuration.
    
    Provides methods to verify broker connectivity, topic existence,
    and consumer group functionality.
    """

    def __init__(self, bootstrap_servers: str):
        """
        Initialize Kafka connection tester.
        
        Args:
            bootstrap_servers: Comma-separated list of Kafka broker addresses
        """
        self.bootstrap_servers = bootstrap_servers
        self._admin_client: Optional[AdminClient] = None
        
    def test_broker_connection(self) -> bool:
        """
        Test connection to Kafka brokers.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            admin_config = {
                'bootstrap.servers': self.bootstrap_servers,
                'socket.timeout.ms': 10000,
                'api.version.request.timeout.ms': 10000
            }
            self._admin_client = AdminClient(admin_config)
            
            # Get cluster metadata to verify connection
            metadata = self._admin_client.list_topics(timeout=10)
            broker_count = len(metadata.brokers)
            
            logger.info(
                f"Successfully connected to Kafka cluster with {broker_count} broker(s)"
            )
            return True
            
        except KafkaException as e:
            logger.error(f"Failed to connect to Kafka brokers: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error testing Kafka connection: {e}")
            return False

    def check_topic_exists(self, topic: str) -> bool:
        """
        Check if a topic exists in the Kafka cluster.
        
        Args:
            topic: Topic name to check
            
        Returns:
            True if topic exists, False otherwise
        """
        if not self._admin_client:
            if not self.test_broker_connection():
                return False
                
        try:
            metadata = self._admin_client.list_topics(timeout=10)
            topic_exists = topic in metadata.topics
            
            if topic_exists:
                topic_metadata = metadata.topics[topic]
                partition_count = len(topic_metadata.partitions)
                logger.info(
                    f"Topic '{topic}' exists with {partition_count} partition(s)"
                )
            else:
                logger.warning(f"Topic '{topic}' does not exist")
                
            return topic_exists
            
        except Exception as e:
            logger.error(f"Error checking topic existence: {e}")
            return False

    def get_topic_partitions(self, topic: str) -> Optional[int]:
        """
        Get the number of partitions for a topic.
        
        Args:
            topic: Topic name
            
        Returns:
            Number of partitions, or None if topic doesn't exist
        """
        if not self._admin_client:
            if not self.test_broker_connection():
                return None
                
        try:
            metadata = self._admin_client.list_topics(timeout=10)
            
            if topic not in metadata.topics:
                logger.warning(f"Topic '{topic}' not found")
                return None
                
            partition_count = len(metadata.topics[topic].partitions)
            logger.info(f"Topic '{topic}' has {partition_count} partition(s)")
            return partition_count
            
        except Exception as e:
            logger.error(f"Error getting topic partitions: {e}")
            return None

    def test_consumer_connection(
        self,
        topic: str,
        group_id: str,
        auto_offset_reset: str = "earliest"
    ) -> bool:
        """
        Test consumer connectivity by attempting to subscribe to a topic.
        
        Args:
            topic: Topic to subscribe to
            group_id: Consumer group ID
            auto_offset_reset: Offset reset strategy
            
        Returns:
            True if consumer can connect and subscribe, False otherwise
        """
        consumer = None
        try:
            consumer_config = {
                'bootstrap.servers': self.bootstrap_servers,
                'group.id': group_id,
                'auto.offset.reset': auto_offset_reset,
                'enable.auto.commit': False,
                'session.timeout.ms': 10000,
                'max.poll.interval.ms': 300000
            }
            
            consumer = Consumer(consumer_config)
            consumer.subscribe([topic])
            
            # Poll once to trigger subscription
            consumer.poll(timeout=5.0)
            
            # Get assignment to verify subscription
            assignment = consumer.assignment()
            
            if assignment:
                logger.info(
                    f"Consumer successfully subscribed to topic '{topic}' "
                    f"with {len(assignment)} partition(s) assigned"
                )
                return True
            else:
                logger.warning(
                    f"Consumer subscribed but no partitions assigned yet "
                    f"(this is normal for new consumer groups)"
                )
                return True
                
        except KafkaException as e:
            logger.error(f"Failed to create consumer: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error testing consumer connection: {e}")
            return False
        finally:
            if consumer:
                consumer.close()

    def get_consumer_lag(self, topic: str, group_id: str) -> Optional[Dict[int, int]]:
        """
        Get consumer lag for each partition.
        
        Args:
            topic: Topic name
            group_id: Consumer group ID
            
        Returns:
            Dictionary mapping partition ID to lag, or None on error
        """
        consumer = None
        try:
            consumer_config = {
                'bootstrap.servers': self.bootstrap_servers,
                'group.id': f"{group_id}-lag-checker",
                'enable.auto.commit': False
            }
            
            consumer = Consumer(consumer_config)
            
            # Get topic partitions
            metadata = consumer.list_topics(topic, timeout=10)
            if topic not in metadata.topics:
                logger.error(f"Topic '{topic}' not found")
                return None
                
            partitions = metadata.topics[topic].partitions
            lag_info = {}
            
            for partition_id in partitions.keys():
                # Get committed offset for the consumer group
                from confluent_kafka import TopicPartition
                tp = TopicPartition(topic, partition_id)
                
                committed = consumer.committed([tp], timeout=10)
                if committed and committed[0].offset >= 0:
                    committed_offset = committed[0].offset
                else:
                    committed_offset = 0
                
                # Get high watermark (latest offset)
                low, high = consumer.get_watermark_offsets(tp, timeout=10)
                
                # Calculate lag
                lag = high - committed_offset
                lag_info[partition_id] = lag
                
                logger.debug(
                    f"Partition {partition_id}: committed={committed_offset}, "
                    f"high={high}, lag={lag}"
                )
            
            total_lag = sum(lag_info.values())
            logger.info(f"Total consumer lag for group '{group_id}': {total_lag}")
            
            return lag_info
            
        except Exception as e:
            logger.error(f"Error getting consumer lag: {e}")
            return None
        finally:
            if consumer:
                consumer.close()


def validate_kafka_config(
    bootstrap_servers: str,
    topic: str,
    group_id: str,
    auto_offset_reset: str = "earliest"
) -> bool:
    """
    Validate Kafka configuration by testing connectivity and topic existence.
    
    Args:
        bootstrap_servers: Comma-separated list of Kafka broker addresses
        topic: Topic name to validate
        group_id: Consumer group ID
        auto_offset_reset: Offset reset strategy
        
    Returns:
        True if all validations pass, False otherwise
    """
    logger.info("Validating Kafka configuration...")
    
    tester = KafkaConnectionTester(bootstrap_servers)
    
    # Test broker connection
    if not tester.test_broker_connection():
        logger.error("Kafka broker connection test failed")
        return False
    
    # Check topic exists
    if not tester.check_topic_exists(topic):
        logger.error(f"Topic '{topic}' does not exist")
        return False
    
    # Test consumer connection
    if not tester.test_consumer_connection(topic, group_id, auto_offset_reset):
        logger.error("Consumer connection test failed")
        return False
    
    logger.info("All Kafka configuration validations passed")
    return True
