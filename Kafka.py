#!/usr/local/bin/python3.8
import sys
import argparse

from pykafka import KafkaClient
from pykafka.common import OffsetType

if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-w', '--write', action='store_true', help='write mode')
    parser.add_argument('-s', '--hosts', default='localhost:9092', help='comma separated list of Kafka brokers')

    group = parser.add_mutually_exclusive_group(required=True)

    group.add_argument('-l', '--list', action='store_true', help='list topics')
    group.add_argument('-t', '--topic', help='topic name from & to which the data is read & write')

    args = parser.parse_args()

    write_mode = args.write
    bootstrap_servers = args.hosts
    list_topics = args.list
    topic_name = args.topic

    client = KafkaClient(hosts=bootstrap_servers)

    if list_topics:
        for topic in client.topics:
            print(topic.decode())
        sys.exit(0)

    topic = client.topics[topic_name]

    try:
        if write_mode:
            with topic.get_sync_producer() as producer:
                line = input('>')
                while line != '\n':
                    producer.produce(line.encode())
                    line = input('>')
        else:
            consumer = topic.get_simple_consumer(
                consumer_group="mygroup",
                auto_offset_reset=OffsetType.LATEST
            )
            for msg in consumer:
                if msg:
                    print(f'{msg.offset} -> {msg.value.decode()}')
                    consumer.commit_offsets()
    except (EOFError, KeyboardInterrupt) as e:
        pass
