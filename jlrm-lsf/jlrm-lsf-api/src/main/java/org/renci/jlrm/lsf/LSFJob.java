package org.renci.jlrm.lsf;

import java.io.File;

import org.renci.jlrm.Job;

public class LSFJob extends Job {

    protected String queueName;

    protected String project;

    protected Integer wallTime;

    protected Integer hostCount;

    public LSFJob() {
        super();
    }

    public LSFJob(String name, File executable) {
        super(name, executable);
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

    public Integer getWallTime() {
        return wallTime;
    }

    public void setWallTime(Integer wallTime) {
        this.wallTime = wallTime;
    }

    public Integer getHostCount() {
        return hostCount;
    }

    public void setHostCount(Integer hostCount) {
        this.hostCount = hostCount;
    }

}
