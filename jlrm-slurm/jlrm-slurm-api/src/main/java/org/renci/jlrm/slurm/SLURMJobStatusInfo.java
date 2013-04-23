package org.renci.jlrm.slurm;

public class SLURMJobStatusInfo {

    private String jobId;

    private SLURMJobStatusType type;

    private String queue;

    private String jobName;

    public SLURMJobStatusInfo(String jobId, SLURMJobStatusType type, String queue, String jobName) {
        super();
        this.jobId = jobId;
        this.type = type;
        this.queue = queue;
        this.jobName = jobName;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public SLURMJobStatusType getType() {
        return type;
    }

    public void setType(SLURMJobStatusType type) {
        this.type = type;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    @Override
    public String toString() {
        return String.format("SLURMJobStatusInfo [jobId=%s, type=%s, queue=%s, jobName=%s]", jobId, type, queue,
                jobName);
    }

}
