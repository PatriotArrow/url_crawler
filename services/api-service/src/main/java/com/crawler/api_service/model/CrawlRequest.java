package com.crawler.api_service.model;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

//This is the model of the crawl request
@Data
public class CrawlRequest {
    @NotBlank(message = "URL cannot be blank")
    private String url;

    @Min(value = 1, message = "Depth must be at least 1")
    @Max(value = 5, message = "Depth cannot exceed 5")
    private int depth;
}
