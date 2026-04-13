package com.dataplatform.bq_cdb_ch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@ConfigurationPropertiesScan("com.dataplatform.bq_cdb_ch")
public class BqCdbChApplication {

    public static void main(String[] args) {
        SpringApplication.run(BqCdbChApplication.class, args);
    }

}