package io.elken.kafkatestdemo;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.assertj.core.api.Condition;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.core.StreamsBuilderFactoryBean;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = {KafkaEmbeddedTest.Config.class})
@RunWith(SpringRunner.class)
public class KafkaEmbeddedTest {

	private static final Logger log = LoggerFactory.getLogger(KafkaEmbeddedTest.class);

	private static final String TRUE_OUTPUT = "test-output-2";
	private static final String FALSE_OUTPUT = "test-output-1";
	private static final String TRUE_FALSE_INPUT_TOPIC = "test-input";

	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, false, TRUE_FALSE_INPUT_TOPIC, FALSE_OUTPUT, TRUE_OUTPUT);

	private Consumer<String, String> trueConsumer;
	private Consumer<String, String> falseConsumer;
	private Consumer<String, String> consumeFromAllTopicsConsumer;

	private KafkaTemplate<String, String> kafkaTemplate;

	@Before
	public void setup() throws Exception {
		falseConsumer = consumeFromAnEmbeddedTopic(FALSE_OUTPUT);
		trueConsumer = consumeFromAnEmbeddedTopic(TRUE_OUTPUT);

		consumeFromAllTopicsConsumer = consumeFromAllEmbeddedTopics();

		kafkaTemplate = kafkaTemplate();
	}

	@Test
	public void test() throws Exception {
		for (int i = 0; i < 5; i++) {
			runTest();
		}
	}

	private void runTest() throws Exception {
		Random random = new Random();
		for (int i = 0; i < 3; i++) {
			kafkaTemplate.send(TRUE_FALSE_INPUT_TOPIC, UUID.randomUUID().toString(), String.valueOf(random.nextBoolean()));
		}

		Map<String, String> trueMessages = consumeAll(trueConsumer);
		Map<String, String> falseMessages = consumeAll(falseConsumer);
		Map<String, String> allMessages = consumeAll(consumeFromAllTopicsConsumer);

		assertThat(allMessages.values()).filteredOn(new Condition<String>() {
			@Override
			public boolean matches(String value) {
				return String.valueOf(true).equals(value);
			}
		}).containsExactlyElementsOf(trueMessages.values());

		assertThat(allMessages.values()).filteredOn(new Condition<String>() {
			@Override
			public boolean matches(String value) {
				return String.valueOf(false).equals(value);
			}
		}).containsExactlyElementsOf(falseMessages.values());
	}

	private Map<String, String> consumeAll(Consumer<String, String> consumer) {
		Map<String, String> allMessages = new HashMap<>();
		for (int i = 0; i < 100; i++) {
			ConsumerRecords<String, String> poll = consumer.poll(10);
			poll.forEach(stringObjectConsumerRecord -> {
				log.error(stringObjectConsumerRecord.topic() + "\n");
				log.error(stringObjectConsumerRecord.partition() + "\n");
				log.error(stringObjectConsumerRecord.offset() + "\n");
				log.error(stringObjectConsumerRecord.headers() + "\n");
				log.error(stringObjectConsumerRecord.value() + "\n");
				allMessages.put(stringObjectConsumerRecord.key(), stringObjectConsumerRecord.value());
			});
		}
		return allMessages;
	}

	private Consumer<String, String> consumeFromAnEmbeddedTopic(String topic) {
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(UUID.randomUUID().toString(), "false", embeddedKafka);
		consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10000);

		DefaultKafkaConsumerFactory<String, String> kafkaConsumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps, new StringDeserializer(), new StringDeserializer());
		Consumer<String, String> consumer = kafkaConsumerFactory.createConsumer();
		try {
			// Consumer setup with consumeFromAnEmbeddedTopic does not work, messages get dropped
			embeddedKafka.consumeFromAnEmbeddedTopic(consumer, topic);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return consumer;
	}

	private Consumer<String, String> consumeFromAllEmbeddedTopics() {
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(UUID.randomUUID().toString(), "false", embeddedKafka);
		consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10000);

		DefaultKafkaConsumerFactory<String, String> kafkaConsumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps, new StringDeserializer(), new StringDeserializer());
		Consumer<String, String> consumer = kafkaConsumerFactory.createConsumer();
		try {
			// Consumer setup with consumeFromAllEmbeddedTopics gets all the messages
			embeddedKafka.consumeFromAllEmbeddedTopics(consumer);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return consumer;
	}

	private KafkaTemplate<String, String> kafkaTemplate() {
		Map<String, Object> senderProperties = KafkaTestUtils.senderProps(embeddedKafka.getBrokersAsString());
		senderProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		senderProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

		ProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(senderProperties);
		return new KafkaTemplate<>(producerFactory);
	}

	@Configuration
	@EnableKafka
	public static class Config {

		@Value("${spring.embedded.kafka.brokers}")
		private String brokers;

		@Bean
		public Map<String, Object> streamsConfig() {
			Map<String, Object> props = new HashMap<>();
			props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0L);
			props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
			return props;
		}

		@Bean
		public FactoryBean<StreamsBuilder> streamsBuilder() {
			Map<String, Object> props = new HashMap<>(streamsConfig());
			props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
			return new StreamsBuilderFactoryBean(new StreamsConfig(props));
		}

		@Bean
		public KStream<String, String> trueFalseStream(StreamsBuilder streamsBuilder) {
			KStream<String, String> trueFalseStream = streamsBuilder
					.stream(TRUE_FALSE_INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

			KStream<String, String>[] branches = trueFalseStream.branch(
					(key, value) -> String.valueOf(true).equals(value),
					(key, value) -> String.valueOf(false).equals(value));

			branches[0].to(TRUE_OUTPUT, Produced.with(Serdes.String(), Serdes.String()));
			branches[1].to(FALSE_OUTPUT, Produced.with(Serdes.String(), Serdes.String()));

			return trueFalseStream;
		}
	}
}
