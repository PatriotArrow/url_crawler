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

import com.crawler.crawler_worker.model.CrawlMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;


@Service
public class CrawlerService {
    
    private static final String TOPIC_NAME = "crawl-jobs";
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final RedisTemplate<String, String> redisTemplate;
    private final ObjectMapper mapper;
    private static final Logger log = LoggerFactory.getLogger(CrawlerService.class);


    public CrawlerService(
        KafkaTemplate<String, String> kafkaTemplate,
        RedisTemplate<String, String> redisTemplate,
        ObjectMapper mapper

    ){
        this.kafkaTemplate = kafkaTemplate;
        this.redisTemplate = redisTemplate;
        this.mapper = mapper;
    }


    public void processCrawlJob(String messageJson) {

        CrawlMessage message;
        try {
            message = this.mapper.readValue(messageJson, CrawlMessage.class);

            //Extract fields heree
            String jobId = message.getJobId();
            String url = message.getUrl();
            int depth = message.getDepth();


            //Check if visited
            Boolean visited = redisTemplate.hasKey(url);
            if (Boolean.TRUE.equals(visited)) {
                log.info("Skipping already visited URL: {}", url);
                return;
            }
            redisTemplate.opsForValue().set(url, "visited");

            //Check depth limit
            if(depth > 0)
            {
                depth--;
                log.info("Crawling URL: {}", url);
                //fetch page
                Document doc;
                try {
                    doc = Jsoup.connect(url).get();
                    Elements links = doc.select("a[href]");
                    for (Element link : links){

                        String href = link.attr("abs:href");
                        // kafkaTemplate.send("crawl-jobs", href, href);

                        CrawlMessage messageCurrent = new CrawlMessage(jobId, href, depth);
                        try {
                            String nextMessageJson = this.mapper.writeValueAsString(messageCurrent);
                            kafkaTemplate.send(TOPIC_NAME, jobId, nextMessageJson);
                        } catch (JsonProcessingException e) {

                            log.error("Failed to serialize crawl message for jobId: {}", jobId, e);
                        }
                        
                    }
                    kafkaTemplate.send("crawl-results", url, "crawled: " + links.size() + " links found");
                
                } catch (Exception e) {
                    log.error("Failed to crawl URL: {}", url, e);
                }


            }else
            {
                //dont queue
                log.info("Depth limit reached, not re-queuing: {}", url);
            }
            
        } catch (Exception e) {
            log.error("Failed to deserialize crawl message", e);
            return;
        }

        //String url = message.getUrl();

        
        

    }
}
