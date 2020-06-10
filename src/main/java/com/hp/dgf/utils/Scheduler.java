package com.hp.dgf.utils;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class Scheduler {
    @Scheduled(cron = "0 1 1 * * ?", zone = "America/New_York")
    public void cronJobSch() {

    }
}