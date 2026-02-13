package com.assn.tcap.ingestor.consumer;

import com.assn.tcap.ingestor.model.TradeDTO;
import com.assn.tcap.ingestor.repo.TradeRepo;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.LocalDate;
import java.util.concurrent.TimeUnit;

import static com.assn.tcap.ingestor.config.KafkaConfig.TOPIC_NAME;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE
)
@Testcontainers
class TradeConsumerContainerIT {

    @Container
    static KafkaContainer kafka =
            new KafkaContainer(
                    DockerImageName.parse("confluentinc/cp-kafka:7.5.0")
            );

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {

        registry.add("spring.kafka.bootstrap-servers",
                kafka::getBootstrapServers);
        registry.add("spring.kafka.consumer.auto-offset-reset",
                () -> "earliest");
        registry.add("spring.kafka.consumer.group-id",
                () -> "container-test-group");
    }

    @Autowired
    private KafkaTemplate<String, TradeDTO> kafkaTemplate;

    @Autowired
    private TradeRepo tradeRepo;

    @Test
    void shouldConsumeAndPersistTrades() throws Exception {

        TradeDTO trade = TradeDTO.builder()
                .tradeId(100L)
                .version(1L)
                .maturityDate(LocalDate.now().plusDays(10))
                .bookId("BOOK1")
                .counterPartyId("CP1")
                .build();

        kafkaTemplate.send(TOPIC_NAME,
                trade.getTradeId().toString(),
                trade).get();

        Awaitility.await()
                .atMost(20, TimeUnit.SECONDS)
                .untilAsserted(() ->
                        assertThat(tradeRepo.findAll()).hasSize(1)
                );
    }
}