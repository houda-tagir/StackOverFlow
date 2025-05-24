package com.sightstack;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class SightStackApplication {

    public static void main(String[] args) {
        SpringApplication.run(SightStackApplication.class, args);
    }
}