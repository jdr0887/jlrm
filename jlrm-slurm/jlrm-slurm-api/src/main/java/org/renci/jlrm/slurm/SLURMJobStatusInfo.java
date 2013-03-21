package org.renci.jlrm.slurm;

public class SLURMJobStatusInfo {

    private String jobId;

    private SLURMJobStatusType type;

    private String queue;

    public SLURMJobStatusInfo(String jobId, SLURMJobStatusType type, String queue) {
        super();
        this.jobId = jobId;
        this.type = type;
        this.queue = queue;
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
        return "SGEJobStatusInfo [jobId=" + jobId + ", type=" + type + ", queue=" + queue + "]";
    }

}
