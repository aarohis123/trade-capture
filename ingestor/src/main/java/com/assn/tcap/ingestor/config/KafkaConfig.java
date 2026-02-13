package com.assn.tcap.ingestor.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;

@Configuration
public class KafkaConfig {


    public static final String TOPIC_NAME= "trades";
    @Bean
    public NewTopic tradeTopic(){
        return TopicBuilder.name(TOPIC_NAME)
                .partitions(2)
                .replicas(1)
                .build();

    }

    @Bean
    public NewTopic tradeDLTTopic(){
        return TopicBuilder.name(TOPIC_NAME + ".DLT")
                .partitions(2)
                .replicas(1)
                .build();
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> batchKafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory,
            DefaultErrorHandler errorHandler) {

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);
        factory.setBatchListener(true);
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }
}
