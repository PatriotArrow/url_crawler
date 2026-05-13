package com.crawler.api_service.service;

import org.springframework.stereotype.Service;

import com.crawler.api_service.model.CrawlRequest;

import org.springframework.kafka.core.KafkaTemplate;

import java.util.UUID;

@Service
public class CrawlService {
    private final KafkaTemplate<String, String> kafkaTemplate;

    public CrawlService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public String submitCrawlJob( CrawlRequest request){

        String jobID = UUID.randomUUID().toString();

        //Job is defined in kafkaTopicConfig
        kafkaTemplate.send("crawl-jobs", jobID, request.getUrl());

        return jobID;
    }

}