package com.github.framiere;

import com.google.common.base.Stopwatch;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.processor.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.LongAdder;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.io.FileUtils.byteCountToDisplaySize;

/*
 ./confluent destroy
 ./confluent start kafka
 ./kafka-topics --zookeeper localhost:2181 --create --topic input --partitions 4 --replication-factor 1
 ./kafka-topics --zookeeper localhost:2181 --create --topic output --partitions 4 --replication-factor 1
 ./kafka-producer-perf-test --topic input \
 --num-records 40000000 \
 --record-size 200 \
 --throughput 15000000 \
 --producer-props \
 acks=1 \
 bootstrap.servers=localhost:9092 \
 buffer.memory=67108864 \
 compression.type=snappy \
 batch.size=32784

 run application java -jar *.jar
 */
public class StreamAndStateStore11 {

	public static final String STATE_STORE = "store";
	public static final boolean storeCaching = true;
	public static boolean storeReplication = true;

	public void stream(String bootstrapServers) {
		Properties properties = new Properties();
		properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
		properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "simple-stream" + new Random().nextInt());
		properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5 * 1000);
		properties.put(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, 100000);
		properties.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
		properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class);

		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384 * 2);
		properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		properties.put(ProducerConfig.ACKS_CONFIG, "all");

		StoreBuilder<KeyValueStore<byte[], byte[]>> store = Stores.keyValueStoreBuilder(
				Stores.persistentKeyValueStore(STATE_STORE),
				Serdes.ByteArray(),
				Serdes.ByteArray());
		if (storeCaching) {
			store.withCachingEnabled();
		}

		if (storeReplication) {
			store.loggingEnabled();
		} else {
			store.withLoggingDisabled();
		}

		Topology topology = new Topology()
				.addSource("Source", "input")
				.addProcessor("Processor", () -> new AbstractProcessor<byte[], byte[]>() {
					private KeyValueStore<byte[], byte[]> store;
					private final LongAdder nbValues = new LongAdder();
					private final LongAdder size = new LongAdder();
					private final Stopwatch stopwatch = Stopwatch.createStarted();

					@Override
					public void init(ProcessorContext context) {
						super.init(context);
						store = (KeyValueStore<byte[], byte[]>) context.getStateStore(STATE_STORE);

						// Punctuator function will be called on the same thread
						context().schedule(SECONDS.toMillis(1), PunctuationType.WALL_CLOCK_TIME, this::flush);
					}

					private void flush(long timestamp) {
						long elapsed = stopwatch.elapsed(SECONDS);
						if (elapsed != 0) {
							long nb = nbValues.longValue();
							System.out.println((nb / elapsed) + " events/s, " + byteCountToDisplaySize(size.longValue() / elapsed) + "/s");
						}
						nbValues.reset();
						size.reset();
						stopwatch.reset();
						stopwatch.start();
					}

					@Override
					public void process(byte[] key, byte[] value) {
						nbValues.increment();
						size.add(key == null ? 0 : key.length);
						size.add(value == null ? 0 : value.length);
						if (value != null) {
							store.put(("" + value.length).getBytes(), value);
						}

						context().forward(key, value);
					}
				}, "Source")
				.addSink("Sink", "output", "Processor")
				.addStateStore(store, "Processor");


		System.out.println(topology.describe());

		KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
		kafkaStreams.cleanUp();
		kafkaStreams.start();
	}

	public static void main(String[] args) {
		String bootstrapServers = args.length == 1 ? args[0] : "localhost:9092";
		System.out.println(bootstrapServers);
		new StreamAndStateStore11().stream(bootstrapServers);
	}
}
