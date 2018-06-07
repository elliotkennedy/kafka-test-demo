package io.elken.springkafkaexamples.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;

import java.util.HashMap;
import java.util.Map;

@EnableKafkaStreams
@Configuration
public class BranchingStream {

	public static final String TRUE_TOPIC = "true-output-topic";

	public static final String FALSE_TOPIC = "false-output-topic";

	public static final String TRUE_FALSE_INPUT_TOPIC = "input-topic";

	@Value("${kafka.brokers:#{'localhost:9092'}}")
	private String brokerAddresses;

	@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
	public StreamsConfig kStreamsConfigs() {
		Map<String, Object> props = new HashMap<>();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "spring-kafka-examples");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerAddresses);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		return new StreamsConfig(props);
	}

	@Bean
	@SuppressWarnings("unchecked")
	public KStream<String, String> trueFalseStream(StreamsBuilder streamsBuilder) {
		KStream<String, String> trueFalseStream = streamsBuilder
				.stream(TRUE_FALSE_INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

		KStream<String, String>[] branches =
				trueFalseStream.branch((key, value) -> String.valueOf(true).equals(value),
						(key, value) -> String.valueOf(false).equals(value));

		branches[0].to(TRUE_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
		branches[1].to(FALSE_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

		return trueFalseStream;
	}
}
