package com.crawler.crawler_worker.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.crawler.crawler_worker.service.CrawlerService;

@Component
public class CrawlJobConsumer {
    private final CrawlerService crawlerService;
    private static final Logger log = LoggerFactory.getLogger(CrawlJobConsumer.class);

    public CrawlJobConsumer(CrawlerService crawlerService) {
        this.crawlerService = crawlerService;

    }

    @KafkaListener(topics = "crawl-jobs", groupId = "crawler-worker-group")
    void consumeCrawlJob(String messageJson) {
        
        log.info("Received crawl job: {}", messageJson);
        
        this.crawlerService.processCrawlJob(messageJson);

    }
}

