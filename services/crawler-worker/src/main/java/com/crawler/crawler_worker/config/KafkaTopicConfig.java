package com.crawler.crawler_worker.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {
    @Bean
    NewTopic crawlTopic() {
        return new NewTopic("crawl-jobs", 1, (short) 1);
    }
    
    @Bean
    NewTopic crawlResultsTopic() {
        return new NewTopic("crawl-results", 1, (short) 1);
    }
}
