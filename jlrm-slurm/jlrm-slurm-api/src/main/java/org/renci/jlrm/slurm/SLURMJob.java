package org.renci.jlrm.slurm;

import org.renci.jlrm.Job;

public class SLURMJob extends Job {

    private static final long serialVersionUID = -2381336994166389859L;

    protected String queueName;

    protected String project;

    protected Long wallTime;

    protected Integer hostCount;

    protected String constraint;

    public SLURMJob() {
        super();
    }

    public SLURMJob(SLURMJobBuilder builder) {
        super();
        this.id = builder.id();
        this.name = builder.name();
        this.executable = builder.executable();
        this.submitFile = builder.submitFile();
        this.output = builder.output();
        this.error = builder.error();
        this.numberOfProcessors = builder.numberOfProcessors();
        this.memory = builder.memory();
        this.disk = builder.disk();
        this.duration = builder.duration();
        this.durationTimeUnit = builder.durationTimeUnit();
        this.constraint = builder.constraint();
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

    public String getConstraint() {
        return constraint;
    }

    public void setConstraint(String constraint) {
        this.constraint = constraint;
    }

}
