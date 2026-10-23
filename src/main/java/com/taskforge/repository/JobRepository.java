package com.taskforge.repository;


import com.taskforge.model.Job;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Repository
public class JobRepository {
    private final ConcurrentMap<String, Job> jobs = new ConcurrentHashMap<>();

    public Job save(Job job){
        jobs.put(job.getId(), job);
        return job;
    }

    public Optional<Job> findById(String id){
        return Optional.ofNullable(jobs.get(id));
    }

    public List<Job> findAll(){
        return List.copyOf(jobs.values());
    }

    public void deleteById(String id){
        jobs.remove(id);
    }

    public boolean existsById(String id){
        return jobs.containsKey(id);
    }
}
