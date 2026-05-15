package com.crawler.crawler_worker.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;


@Service
public class CrawlerService {
    
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final RedisTemplate<String, String> redisTemplate;
    private static final Logger log = LoggerFactory.getLogger(CrawlerService.class);


    public CrawlerService(
        KafkaTemplate<String, String> kafkaTemplate,
        RedisTemplate<String, String> redisTemplate

    ){
        this.kafkaTemplate = kafkaTemplate;
        this.redisTemplate = redisTemplate;
    }


    public void processCrawlJob(String url) {

        Boolean visited = redisTemplate.hasKey(url);
        if (Boolean.TRUE.equals(visited)) {
            log.info("Skipping already visited URL: {}", url);
            return;
        }
        redisTemplate.opsForValue().set(url, "visited");

        log.info("Crawling URL: {}", url);
        //fetch page
        Document doc;
        try {
            doc = Jsoup.connect(url).get();
            Elements links = doc.select("a[href]");
            for (Element link : links){
                String href = link.attr("abs:href");
                kafkaTemplate.send("crawl-jobs", href, href);
            }
            kafkaTemplate.send("crawl-results", url, "crawled: " + links.size() + " links found");
            
        } catch (Exception e) {
            log.error("Failed to crawl URL: {}", url, e);
        }
        

    }
}
