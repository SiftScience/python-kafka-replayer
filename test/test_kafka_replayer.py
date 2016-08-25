import collections
import kafka_replayer
import unittest

FakeConsumerRecord = collections.namedtuple('FakeConsumerRecord',
                                            ['offset', 'timestamp', 'topic', 'partition'])

class FakeConsumer(object):
    def __init__(self, advance=False):
        self._records = [self._fake_record(0, 0),
                         self._fake_record(1, 1),
                         self._fake_record(1, 2),
                         self._fake_record(2, 3),
                         self._fake_record(3, 4),
                         self._fake_record(4, 5),
                         self._fake_record(5, 6)]
        self._advance = advance
        self._index = 0
        self.seek_count = 0
        self.pause_set = set()

    def _fake_record(self, ts, offset):
        return FakeConsumerRecord(offset, ts, 'fake', 0)

    def seek(self, tp, offset):
        self._index = offset
        self.seek_count += 1

    def pause(self, tp):
        self.pause_set.add(tp)

    def paused(self):
        return self.pause_set

    def assign(self, partitions):
        pass

    def close(self):
        pass

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def next(self):
        if self._index >= 0 and self._index < len(self._records):
            record = self._records[self._index]
            if self._advance:
                self._index += 1
            return record
        raise StopIteration

class TestKafkaReplayer(unittest.TestCase):
    def test_binary_search(self):
        replayer = kafka_replayer.KafkaReplayer('fake')
        # search for timestamp 1, which should return offset 1, the first offset with ts = 1
        consumer = FakeConsumer()
        offset = replayer._binary_search(consumer, ('fake', 0), 0, 6, 1)
        self.assertEqual(offset, 1)
        self.assertEqual(consumer.seek_count, 3)  # 3, 1, and 0

    def test_replay(self):
        replayer = kafka_replayer.KafkaReplayer('fake')
        consumer = FakeConsumer(advance=True)
        replayer._find_seek_points = lambda x : {0: 1}
        replayer._create_consumer = lambda: consumer
        replayer._all_partitions_set = lambda x : set([0])
        # search between timestamps 1 and 4, which is 5 records
        records = [x for x in replayer.replay(1, 4)]
        self.assertEqual(len(records), 5)
        self.assertEqual(records[0].offset, 1)
        self.assertEqual(records[1].offset, 2)
        self.assertEqual(records[2].offset, 3)
        self.assertEqual(records[3].offset, 4)
        self.assertEqual(records[4].offset, 5)
