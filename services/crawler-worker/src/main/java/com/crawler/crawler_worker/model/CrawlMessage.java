package com.crawler.crawler_worker.model;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

//This is the model of the crawl message
@Data @NoArgsConstructor @AllArgsConstructor 
public class CrawlMessage {

    private String jobId;
    private String url;
    private int depth;
}
