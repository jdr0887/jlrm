package org.renci.jlrm.lsf;

public class LSFJobStatusInfo {

    private String jobId;

    private LSFJobStatusType type;

    private String queue;

    public LSFJobStatusInfo(String jobId, LSFJobStatusType type, String queue) {
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

    public LSFJobStatusType getType() {
        return type;
    }

    public void setType(LSFJobStatusType type) {
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
        return "LSFJobStatusInfo [jobId=" + jobId + ", type=" + type + ", queue=" + queue + "]";
    }

}
