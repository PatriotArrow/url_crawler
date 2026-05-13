# URL Crawler

A distributed URL crawler built with Java and Spring Boot. You submit a URL and it crawls the page, extracts all the links, and keeps going until it hits the depth limit. Built to learn how microservices talk to each other through Kafka without being coupled. Uses Redis to avoid crawling the same URL twice and Postgres to store results. 
Still a work in progress.
