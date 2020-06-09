package com.hp.dgf;

import com.hp.dgf.model.FileStorageProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties({
        FileStorageProperties.class
})
public class HpDgfApplication {
    public static void main(String[] args) {
        SpringApplication.run(HpDgfApplication.class, args);
    }
}
