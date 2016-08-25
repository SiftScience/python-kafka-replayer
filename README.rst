*********************
Python Kafka Replayer
*********************

.. image:: https://circleci.com/gh/SiftScience/python-kafka-replayer/tree/master.svg?style=svg
    :target: https://circleci.com/gh/SiftScience/python-kafka-replayer/tree/master

kafka_replayer is a library that helps consume time ranges of messages from Kafka topics. While the
standard Kafka consumer API allows seeking to a specific offset and replaying from there, using
offsets as the replay abstraction is cumbersome and potentially error-prone. This library does the
translation from timestamps to offsets transparently.

This library is written in Python, and leverages `kafka-python`_'s consumer to poll Kafka for messages.

==========
Installing
==========
.. code-block:: bash

    $ pip install kafka_replayer

=====
Using
=====
.. code-block:: python

    import json
    import kafka_replayer
    
    des_fn = lambda x: json.loads(x) if x else None
    replayer = kafka_replayer.KafkaReplayer('my-topic',
                                            bootstrap_servers=['localhost:9092'],
                                            key_deserializer=des_fn,
                                            value_deserializer=des_fn)

    # Replay all records between the start and end millis timestamps
    for record in replayer.replay(1469467314341, 1469467907549):
        print record

=============
Documentation
=============

http://pythonhosted.org/kafka_replayer/

=======
License
=======

See `LICENSE <https://github.com/SiftScience/python-kafka-replayer/blob/master/LICENSE>`_.

.. _kafka-python: https://github.com/dpkp/kafka-python
