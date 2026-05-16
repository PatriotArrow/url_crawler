package com.crawler.api_service.service;

import org.springframework.stereotype.Service;

import com.crawler.api_service.model.CrawlMessage;
import com.crawler.api_service.model.CrawlRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.UUID;

@Service
public class CrawlService {

    private static final String TOPIC_NAME = "crawl-jobs";
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper mapper;

    private static final Logger log = LoggerFactory.getLogger(CrawlService.class);

    public CrawlService(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper mapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.mapper = mapper;
    }

    public String submitCrawlJob( CrawlRequest request){

        String jobId = UUID.randomUUID().toString();

        CrawlMessage message = new CrawlMessage(jobId, request.getUrl(), request.getDepth());
        
        try {
            String messageJson = this.mapper.writeValueAsString(message);
            kafkaTemplate.send(TOPIC_NAME, jobId, messageJson);
        } catch (JsonProcessingException e) {

            log.error("Failed to serialize crawl message for jobId: {}", jobId, e);
        }

        //Job is defined in kafkaTopicConfig        

        return jobId;
    }

}