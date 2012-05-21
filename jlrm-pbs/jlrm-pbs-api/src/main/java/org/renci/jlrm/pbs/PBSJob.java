package org.renci.jlrm.pbs;

import java.io.File;

import org.renci.jlrm.Job;

public class PBSJob extends Job {

    protected String queueName = "default";

    protected String project;

    protected Long wallTime;

    protected Integer hostCount;

    public PBSJob() {
        super();
    }

    public PBSJob(String name, File executable) {
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
