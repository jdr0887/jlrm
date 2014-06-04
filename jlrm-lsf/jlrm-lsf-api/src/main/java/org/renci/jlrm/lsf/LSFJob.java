package org.renci.jlrm.lsf;

import org.renci.jlrm.Job;

public class LSFJob extends Job {

    protected String queueName;

    protected String project;

    protected Long wallTime;

    protected Integer hostCount;

    public LSFJob() {
        super();
    }

    public LSFJob(LSFJobBuilder builder) {
        super();
        // from JobBuilder
        this.id = builder.id();
        this.name = builder.name();
        this.executable = builder.executable();
        this.submitFile = builder.submitFile();
        this.output = builder.output();
        this.error = builder.error();
        this.numberOfProcessors = builder.numberOfProcessors();
        this.memory = builder.memory();
        this.duration = builder.duration();
        this.durationTimeUnit = builder.durationTimeUnit();
        // from LSFJobBuilder
        this.queueName = builder.queueName();
        this.project = builder.project();
        this.wallTime = builder.wallTime();
        this.hostCount = builder.hostCount();
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public Long getWallTime() {
        return wallTime;
    }

    public void setWallTime(Long wallTime) {
        this.wallTime = wallTime;
    }

    public Integer getHostCount() {
        return hostCount;
    }

    public void setHostCount(Integer hostCount) {
        this.hostCount = hostCount;
    }

}
