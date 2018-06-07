package io.elken.springkafkaexamples.streams;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.elken.springkafkaexamples.streams.BranchingStream.FALSE_TOPIC;
import static io.elken.springkafkaexamples.streams.BranchingStream.TRUE_FALSE_INPUT_TOPIC;
import static io.elken.springkafkaexamples.streams.BranchingStream.TRUE_TOPIC;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@DirtiesContext
@SpringBootTest(properties = "kafka.brokers=${" + KafkaEmbedded.SPRING_EMBEDDED_KAFKA_BROKERS + "}",
		classes = {BranchingStream.class, BranchingStreamTest.Config.class})
@EmbeddedKafka(partitions = 1,
		topics = {
				TRUE_TOPIC,
				FALSE_TOPIC,
				TRUE_FALSE_INPUT_TOPIC})
public class BranchingStreamTest {

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	private KafkaEmbedded kafkaEmbedded;

	@Test
	public void testBranchingStream() throws Exception {
		Consumer<String, String> falseConsumer = createConsumer();
		this.kafkaEmbedded.consumeFromAnEmbeddedTopic(falseConsumer, FALSE_TOPIC);

		Consumer<String, String> trueConsumer = createConsumer();
		this.kafkaEmbedded.consumeFromAnEmbeddedTopic(trueConsumer, TRUE_TOPIC);

		this.kafkaTemplate.sendDefault(String.valueOf(true));
		this.kafkaTemplate.sendDefault(String.valueOf(true));
		this.kafkaTemplate.sendDefault(String.valueOf(false));

		ConsumerRecords<String, String> trueRecords = KafkaTestUtils.getRecords(trueConsumer);
		ConsumerRecords<String, String> falseRecords = KafkaTestUtils.getRecords(falseConsumer);

		List<String> trueValues = new ArrayList<>();
		trueRecords.forEach(trueRecord -> trueValues.add(trueRecord.value()));

		List<String> falseValues = new ArrayList<>();
		falseRecords.forEach(falseRecord -> falseValues.add(falseRecord.value()));

		assertThat(trueValues).containsExactly("true", "true");
		assertThat(falseValues).containsExactly("false");

		falseConsumer.close();
		trueConsumer.close();
	}

	private Consumer<String, String> createConsumer() {
		Map<String, Object> consumerProps =
				KafkaTestUtils.consumerProps(UUID.randomUUID().toString(), "false", this.kafkaEmbedded);
		consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10000);

		DefaultKafkaConsumerFactory<String, String> kafkaConsumerFactory =
				new DefaultKafkaConsumerFactory<>(consumerProps, new StringDeserializer(), new StringDeserializer());
		return kafkaConsumerFactory.createConsumer();
	}

	@Configuration
	public static class Config {

		@Value("${kafka.brokers}")
		private String brokerAddresses;

		@Bean
		public ProducerFactory<Integer, String> producerFactory() {
			return new DefaultKafkaProducerFactory<>(producerConfigs());
		}

		@Bean
		public Map<String, Object> producerConfigs() {
			return KafkaTestUtils.senderProps(this.brokerAddresses);
		}

		@Bean
		public KafkaTemplate<?, ?> kafkaTemplate() {
			KafkaTemplate<Integer, String> kafkaTemplate = new KafkaTemplate<>(producerFactory());
			kafkaTemplate.setDefaultTopic(TRUE_FALSE_INPUT_TOPIC);
			return kafkaTemplate;
		}
	}
}
