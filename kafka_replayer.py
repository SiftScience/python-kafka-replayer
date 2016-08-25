import kafka
import logging
import six
import time

class KafkaReplayer(object):
    """A utility that allows replaying Kafka records by time range."""

    def __init__(self, topic_name, partitions=None, **configs):
        """Create the replayer.

        Args:
            topic_name: The topic to replay
            partitions: Optionally specify the set of partitions (ints) to replay
            configs: The configuration kwargs to pass forward to kafka.KafkaConsumer
        """
        if not topic_name:
            raise ValueError('topic_name is required')
        self._topic_name = topic_name
        self._partitions = partitions
        self._configs = self._configs_with_defaults(configs)
        self._log_interval = 10000
        self._logger = self._create_logger()

    def _create_logger(self):
        self._logger = logging.getLogger(__name__)
        null_handler = logging.NullHandler()
        null_handler.setLevel(logging.DEBUG)
        self._logger.addHandler(null_handler)
        return self._logger

    def _configs_with_defaults(self, configs):
        if 'group_id' not in configs:
            configs['group_id'] = None
        if 'consumer_timeout_ms' not in configs:
            configs['consumer_timeout_ms'] = 10000
        return configs

    def _get_time_millis(self):
        return int(round(time.time() * 1000))

    def _create_consumer(self):
        return kafka.KafkaConsumer(**(self._configs))

    def _find_seek_points(self, start_time):
        seek_points = {}
        consumer = self._create_consumer()
        try:
            topic_partitions = self._topic_partitions_for_set(self._all_partitions_set(consumer))
            for topic_partition in topic_partitions:
                # We need to compute the offset independently for each partition via binary search
                consumer.assign([topic_partition])
                consumer.seek_to_end(topic_partition)
                end_offset = consumer.position(topic_partition)
                consumer.seek_to_beginning(topic_partition)
                start_offset = consumer.position(topic_partition)
                target_offset = self._binary_search(consumer, topic_partition, start_offset,
                                                    end_offset, start_time)
                seek_points[topic_partition] = target_offset
                self._logger.debug('Start offset for {0} is {1}'.format(
                    topic_partition, target_offset))
            self._logger.info('Start offsets: {0}'.format(seek_points))
            return seek_points
        finally:
            consumer.close()

    def _all_partitions_set(self, consumer):
        all_partitions = consumer.partitions_for_topic(self._topic_name)
        partitions = None
        if self._partitions:
            partitions = all_partitions.intersection(self._partitions)
        else:
            partitions = all_partitions
        return partitions

    def _topic_partitions_for_set(self, partition_set):
        return [kafka.TopicPartition(self._topic_name, p) for p in partition_set]

    def _get_next_if_available(self, consumer):
        record = None
        try:
            record = next(consumer)
        except StopIteration:
            self._logger.debug('Got StopIteration, leaving the record as None')
        return record

    def _binary_search(self, consumer, tp, start, end, target_time):
        # Overall goal: find the earliest offset that is no earlier than the target time
        if start == end:
            return start
        insertion_point = int(start + ((end - start) / 2))
        consumer.seek(tp, insertion_point)
        record = self._get_next_if_available(consumer)
        if record:
            ts = record.timestamp
            if insertion_point == start:
                return start if target_time <= ts else end
            elif ts < target_time:
                return self._binary_search(consumer, tp, insertion_point + 1, end, target_time)
            else:
                return self._binary_search(consumer, tp, start, insertion_point, target_time)
        return start

    def replay(self, start_time, end_time):
        """Replay all specified partitions over the specified time range (inclusive).

        Args:
            start_time: The start timestamp in milliseconds
            end_time: The end timestamp in milliseconds

        Yields:
            The next ConsumerRecord found within the given time range

        Raises:
            ValueError: If the specified start or end time is invalid
        """
        if start_time < 0:
            raise ValueError('start_time must be non-negative')
        if end_time < 0:
            raise ValueError('end_time must be non-negative')
        if start_time > self._get_time_millis():
            raise ValueError('start_time must not be in the future')
        if start_time > end_time:
            raise ValueError('end_time must be at least start_time')
        count = 0
        last_timestamp = 0
        seek_points = self._find_seek_points(start_time)
        consumer = self._create_consumer()
        try:
            # Set up a the consumer to fetch all desired partitions from their seek points
            partitions = self._all_partitions_set(consumer)
            partition_list = self._topic_partitions_for_set(partitions)
            consumer.assign(partition_list)
            for tp, offset in six.iteritems(seek_points):
                consumer.seek(tp, offset)
            while len(partitions) > 0:
                record = self._get_next_if_available(consumer)
                if not record:
                    self._logger.info('No more records available. Terminating.')
                    partitions = set()
                else:
                    last_timestamp = record.timestamp
                    if last_timestamp > end_time:
                        # Since partitions are ordered, if we see a too-new timestamp, mark the
                        # partition complete.
                        partitions.discard(record.partition)
                        tp = kafka.TopicPartition(topic=record.topic, partition=record.partition)
                        if tp not in consumer.paused():
                            consumer.pause(tp)
                            self._logger.debug('Completed partition {0}'.format(tp))
                    elif (record.partition in partitions and last_timestamp >= start_time
                          and last_timestamp <= end_time):
                        # Send the record to the client if it's within the specified time range
                        yield record
                    count += 1
                    if count % self._log_interval == 0:
                        self._logger.debug('Processed {0} offsets, last timestamp: {1}'.format(
                            count, last_timestamp))
        except Exception as e:
            self._logger.error('Unexpected exception: {0}'.format(str(e)))
        finally:
            self._logger.info('Processed {0} offsets, last timestamp: {1}'.format(
                count, last_timestamp))
            consumer.close()

