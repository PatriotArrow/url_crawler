//database access layer

package com.crawler.result_processor.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

import com.crawler.result_processor.model.CrawlResult;

public interface CrawlResultRepository extends JpaRepository<CrawlResult, Long> {
    List<CrawlResult> findByJobId(String jobId);
}
