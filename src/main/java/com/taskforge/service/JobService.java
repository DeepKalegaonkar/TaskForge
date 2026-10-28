package com.taskforge.service;

import com.taskforge.model.Job;
import com.taskforge.model.JobAttempt;
import com.taskforge.repository.JobAttemptRepository;
import com.taskforge.repository.JobRepository;
import com.taskforge.scheduler.JobScheduler;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.RejectedExecutionException;

@Service
public class JobService {

    private final JobRepository jobRepository;
    private final JobAttemptRepository jobAttemptRepository;
    private final JobScheduler jobScheduler;

    public JobService(
            JobRepository jobRepository,
            JobAttemptRepository jobAttemptRepository,
            JobScheduler jobScheduler){
        this.jobRepository = jobRepository;
        this.jobAttemptRepository = jobAttemptRepository;
        this.jobScheduler = jobScheduler;
    }

    public Job submitJob(Job job){
        if(job == null){
            throw new IllegalArgumentException("Job cannot be null");
        }

        jobRepository.save(job);

        try{
            jobScheduler.submit(job);
        } catch(RejectedExecutionException e){
            jobRepository.deleteById(job.getId());
            throw e;
        }
        return job;
    }

    public Optional<Job> getJobById(String id){
        return jobRepository.findById(id);
    }

    public List<Job> getAllJobs(){
        return jobRepository.findAll();
    }

    public Optional<List<JobAttempt>> getAttempts(String id){
        if (!jobRepository.existsById(id)) {
            return Optional.empty();
        }

        return Optional.of(jobAttemptRepository.findByJobId(id));
    }

    public boolean cancelJob(String id) {
        if (jobRepository.findById(id).isEmpty()) {
            return false;
        }

        return jobScheduler.cancel(id);
    }
}
