
package com.taskforge;

import com.taskforge.handler.JobHandler;
import com.taskforge.model.Job;
import com.taskforge.model.JobStatus;
import com.taskforge.scheduler.JobScheduler;

import java.util.concurrent.TimeUnit;

public class Main {


    public static void main(String[] args) throws InterruptedException {

        JobScheduler scheduler = new JobScheduler(1, 5);

        Job blockingJob = new Job("Blocking Job", () -> {
            Thread.sleep(1500);
        });

        Job queuedJob = new Job("Queued Job", () -> {
            System.out.println("ERROR: Cancelled job executed!");
        });

        scheduler.submit(blockingJob);

        // Give the worker time to start the first job.
        while (blockingJob.getStatus() == JobStatus.QUEUED) {
            Thread.sleep(10);
        }

        scheduler.submit(queuedJob);

        boolean cancelled = scheduler.cancel(queuedJob);

        System.out.println("Cancellation successful: " + cancelled);
        System.out.println("Queued job status: " + queuedJob.getStatus());

        scheduler.shutdown();
        scheduler.awaitTermination(5, TimeUnit.SECONDS);

        System.out.println("Blocking job status: " + blockingJob.getStatus());
    }

}
