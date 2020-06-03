package com.hp.dgf;

import com.hp.dgf.model.FileStorageProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties({
        FileStorageProperties.class
})
public class HpDgfApplication {
    public static void main(String[] args) {
        SpringApplication.run(HpDgfApplication.class, args);
    }
}
