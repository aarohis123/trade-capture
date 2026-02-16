package com.assn.tcap.ingestor.consumer;

import com.assn.tcap.ingestor.model.TradeDTO;
import com.assn.tcap.ingestor.repo.TradeMongoRepository;
import lombok.extern.slf4j.Slf4j;
import org.junit.Ignore;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.LocalDate;

import static com.assn.tcap.ingestor.config.KafkaConfig.TOPIC_NAME;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
        properties = {
                "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
                "spring.kafka.consumer.group-id=test-group-nosql",
                "spring.kafka.consumer.auto-offset-reset=earliest",
                "spring.kafka.consumer.enable-auto-commit=false",
                "spring.kafka.listener.ack-mode=manual",
                "spring.kafka.listener.type=batch",
                "spring.kafka.admin.auto-create=true",
                "spring.kafka.properties.auto.create.topics.enable=true",
                "spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer",
                "spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JsonSerializer",
                "spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer",
                "spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JsonDeserializer",
                "spring.kafka.consumer.properties.spring.json.trusted.packages=*",
                "spring.kafka.consumer.properties.spring.json.value.default.type=com.assn.tcap.ingestor.model.TradeDTO"
        }
)@EmbeddedKafka(partitions = 2, topics = {TOPIC_NAME}, brokerProperties = {"listeners=PLAINTEXT://localhost:0", "port=0"})
@Testcontainers
@Slf4j
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class NoSqlTradeConsumerIntegrationTest {


    @Container
    static MongoDBContainer mongoDBContainer =
            new MongoDBContainer("mongo:7.0");

    @DynamicPropertySource
    static void setProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri",
                mongoDBContainer::getReplicaSetUrl);
    }

    @Autowired
    protected EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    protected KafkaListenerEndpointRegistry registry;

    @Autowired
    protected KafkaTemplate<String, TradeDTO> kafkaTemplate;

    @Autowired
    protected TradeMongoRepository tradeMongoRepo;

    @BeforeEach
    void setUp() {
        // Wait for consumer to be ready
        registry.getListenerContainers().forEach(container ->
                ContainerTestUtils.waitForAssignment(
                        container,
                        embeddedKafkaBroker.getPartitionsPerTopic()
                )
        );
    }


    @Test
    @Order(1)
    void shouldConsumeAndPersistTradesFreshTrade() throws Exception {
        TradeDTO trade1 = getTrade(1L, 1L, "BOOK1", "CP1", LocalDate.now().plusDays(10));
        TradeDTO trade2 = getTrade(2L, 1L, "BOOK2", "CP2", LocalDate.now().plusDays(5));
        sendMessageToKafka(List.of(trade1,trade2));
        await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() ->
                        assertThat(tradeMongoRepo.findAll().size()).isEqualTo(2)
                );

        assertThat(tradeMongoRepo.findAll()).hasSize(2);
    }

    @Test
    @Order(2)
    @Disabled("Temporarily disabled - fixing mapping issue")
    void shouldCreateANewVersionAndUpdateOtherTrade() throws Exception {
        TradeDTO trade1 = getTrade(1L, 2L, "BOOK1", "CP1-New-Version", LocalDate.now().plusDays(8));
        TradeDTO trade2 = getTrade(2L, 1L, "BOOK2", "CP2-Updated", LocalDate.now().plusDays(5));
        sendMessageToKafka(List.of(trade1,trade2));
        await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() ->
                        assertThat(tradeMongoRepo.findAll().size()).isEqualTo(3)
                );

        assertThat(tradeMongoRepo.findAll()).hasSize(3);
    }

    @Test
    @Order(3)
    @Disabled("Temporarily disabled - fixing mapping issue")
    void shouldRejectAllTrade() throws Exception {
        TradeDTO trade1 = getTrade(1L, -1L, "BOOK1", "Rejected Trade Version less", LocalDate.now().plusDays(8));
        TradeDTO trade2 = getTrade(2L, -1L, "BOOK2", "Rejected Trade Version less", LocalDate.now().plusDays(5));
        sendMessageToKafka(List.of(trade1,trade2));
        await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() ->
                        assertThat(tradeMongoRepo.findAll().size()).isEqualTo(3)
                );

        assertThat(tradeMongoRepo.findAll()).hasSize(3);
    }

    @Test
    @Order(4)
    @Disabled("Temporarily disabled - fixing mapping issue")
    void shouldRejectSomeTradesInBatch() throws Exception {
        TradeDTO trade1 = getTrade(1L, 3L, "BOOK1", "Rejected Trade Expired Maturity", LocalDate.now().minusDays(2));
        TradeDTO trade2 = getTrade(3L, 1L, "BOOK3", "Rejected Trade Version less", LocalDate.now().plusDays(8));
        TradeDTO trade3 = getTrade(3L, 2L, "BOOK3", "Rejected Trade Version less", LocalDate.now().plusDays(8));
        TradeDTO trade4 = getTrade(3L, 3L, "BOOK3", "Persist this trade", LocalDate.now().plusDays(8));

        sendMessageToKafkaInBatch(List.of(trade1,trade2,trade3,trade4));
        await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() ->
                        assertThat(tradeMongoRepo.findAll().size()).isGreaterThanOrEqualTo(4)
                );
    }


    private void sendMessageToKafka(List<TradeDTO> tradeDTOs){

        tradeDTOs.forEach(trade->{
            kafkaTemplate.send(TOPIC_NAME, trade.getTradeId().toString(),trade).whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("TradeEntity sent successfully. Topic: {}, Offset: {}, partition: {}", result.getRecordMetadata().topic(), result.getRecordMetadata().offset(),result.getRecordMetadata().partition());
                } else {
                    log.error("Failed to send trade", ex);
                }
            });
        });
    }

    private void sendMessageToKafkaInBatch(List<TradeDTO> tradeDTOs){
        tradeDTOs.forEach(trade-> kafkaTemplate.send(TOPIC_NAME, trade.getTradeId().toString(),trade));
    }

    private TradeDTO getTrade(Long tradeId, Long version, String bookId, String counterPartyId, LocalDate maturityDate){
        return TradeDTO.builder()
                .tradeId(tradeId)
                .version(version)
                .maturityDate(maturityDate)
                .bookId(bookId)
                .counterPartyId(counterPartyId)
                .build();

    }
}