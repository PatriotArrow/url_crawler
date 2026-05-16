
/* 
Listens to crawl crawl-results, receives messages, hands them off
*/

package com.crawler.result_processor.consumer;

import java.time.LocalDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import com.crawler.result_processor.model.CrawlResult;
import com.crawler.result_processor.repository.CrawlResultRepository;

@Component
public class CrawlResultConsumer {

    private final CrawlResultRepository crawlResultRepository;

    private static final Logger log = LoggerFactory.getLogger(CrawlResultConsumer.class);

    public CrawlResultConsumer(CrawlResultRepository crawlResultRepository){
        this.crawlResultRepository = crawlResultRepository;
    }

    @KafkaListener(topics = "crawl-results")
    void consumeCrawlResult(
        @Header(KafkaHeaders.RECEIVED_KEY)String jobId,
        @Payload String message
    ){
        try{

            String[] parts = message.split("\\|");
            String url = parts[0];
            int linksFound = Integer.parseInt(parts[1]);

            CrawlResult crawlResult = new CrawlResult();
            crawlResult.setJobId(jobId);
            crawlResult.setUrl(url);
            crawlResult.setLinksFound(linksFound);
            crawlResult.setCrawledAt(LocalDateTime.now()); 

            this.crawlResultRepository.save(crawlResult);
            log.info("Saved crawl result for URL: {}", url);


        }catch (Exception e) {
            log.error("Failed to process crawl result: {}", message, e);
        }

        

    }
}
