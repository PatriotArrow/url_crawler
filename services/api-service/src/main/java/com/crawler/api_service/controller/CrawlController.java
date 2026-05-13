package com.crawler.api_service.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.crawler.api_service.model.CrawlRequest;
import com.crawler.api_service.service.CrawlService;

import jakarta.validation.Valid;


@RestController
@RequestMapping("/api/crawl")
public class CrawlController {
    
    private final CrawlService crawlService;

    public CrawlController(CrawlService crawlService) {
        this.crawlService = crawlService;
    }

    // Endpoint to submit a crawl job at POST /api/crawl
    @PostMapping("/")
    public ResponseEntity<String>  submitCrawlJob (@RequestBody @Valid  CrawlRequest crawlRequest){
        String jobID = crawlService.submitCrawlJob(crawlRequest);
        
        // Return the job ID to the client with a status 202 
        return new ResponseEntity<>(jobID, HttpStatus.ACCEPTED);

    }

    
}
