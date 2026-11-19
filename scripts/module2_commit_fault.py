"""Isolated test only: fail the commit after the consumer's real InfluxDB write."""
import os
import sys

if (os.getenv("MODULE2_TEST_GUARD") != "isolated-module2-verification"
        or os.getenv("SENSOR_TOPIC") != "m2-test-sensor"
        or os.getenv("CONSUMER_GROUP") != "m2-test-writer"):
    raise RuntimeError("Refusing fault injection outside the isolated Module 02 test group")
sys.path.insert(0, "/app/src")
import kafka_consumer

def fail_commit(_consumer, message):
    print(f"INJECTED_COMMIT_FAILURE topic={message.topic} partition={message.partition} offset={message.offset}", flush=True)
    raise RuntimeError("isolated injected commit failure")

kafka_consumer.commit_message = fail_commit
kafka_consumer.consume_and_write()
