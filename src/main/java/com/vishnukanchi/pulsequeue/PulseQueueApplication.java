package com.vishnukanchi.pulsequeue;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class PulseQueueApplication {
    public static void main(String[] args) { SpringApplication.run(PulseQueueApplication.class, args); }
}
