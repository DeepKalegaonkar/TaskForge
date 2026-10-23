package com.taskforge.config;

import com.taskforge.scheduler.JobScheduler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JobExecutionConfig {

    @Bean(destroyMethod = "shutdown")
    public JobScheduler jobScheduler(){
        return new JobScheduler(2, 100);
    }
}
