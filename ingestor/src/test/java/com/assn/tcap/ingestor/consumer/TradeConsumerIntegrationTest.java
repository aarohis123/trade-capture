package com.assn.tcap.ingestor.consumer;

import com.assn.tcap.ingestor.model.TradeDTO;
import com.assn.tcap.ingestor.repo.TradeRepo;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;

import org.springframework.test.context.ActiveProfiles;

import java.time.LocalDate;
import java.util.List;

import static com.assn.tcap.ingestor.config.KafkaConfig.TOPIC_NAME;
import static org.awaitility.Awaitility.await;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
        properties = {
                "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
                "spring.kafka.consumer.auto-offset-reset=earliest",
                "spring.kafka.consumer.group-id=test-group",
                "spring.kafka.admin.auto-create=true",
                "spring.kafka.properties.auto.create.topics.enable=true"
        }
)@EmbeddedKafka(partitions = 2, topics = {TOPIC_NAME})
@DirtiesContext
@Slf4j
class TradeConsumerIntegrationTest {


    @Autowired
    protected EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    protected KafkaListenerEndpointRegistry registry;

    @Autowired
    protected KafkaTemplate<String, TradeDTO> kafkaTemplate;

    @Autowired
    private TradeRepo tradeRepo;
    @Autowired
    private KafkaAdmin kafkaAdmin;


    @Test
    void shouldConsumeAndPersistTrades() throws Exception {

        // Wait for consumer to be ready
        registry.getListenerContainers().forEach(container ->
                ContainerTestUtils.waitForAssignment(
                        container,
                        embeddedKafkaBroker.getPartitionsPerTopic()
                )
        );

        TradeDTO trade1 = TradeDTO.builder()
                .tradeId(1L)
                .version(1L)
                .maturityDate(LocalDate.now().plusDays(10))
                .bookId("BOOK1")
                .counterPartyId("CP1")
                .build();

        TradeDTO trade2 = TradeDTO.builder()
                .tradeId(2L)
                .version(1L)
                .maturityDate(LocalDate.now().plusDays(5))
                .bookId("BOOK2")
                .counterPartyId("CP2")
                .build();

        log.info("Will send the message now...");

        Thread.sleep(1000*10);

        kafkaTemplate.send(TOPIC_NAME, trade1.getTradeId().toString(),trade1).whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Trade sent successfully. Topic: {}, Offset: {}, partition: {}", result.getRecordMetadata().topic(), result.getRecordMetadata().offset(),result.getRecordMetadata().partition());
            } else {
                log.error("Failed to send trade", ex);
            }
        });;
        kafkaTemplate.send(TOPIC_NAME,trade2.getTradeId().toString(), trade2).whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Trade sent successfully. Topic: {}, Offset: {}, partition: {}", result.getRecordMetadata().topic(), result.getRecordMetadata().offset(),result.getRecordMetadata().partition());
            } else {
                log.error("Failed to send trade", ex);
            }
        });

        await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() ->
                        assertThat(tradeRepo.findAll().size()).isEqualTo(2)
                );

        assertThat(tradeRepo.findAll()).hasSize(2);
    }
}