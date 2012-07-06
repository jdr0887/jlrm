package org.renci.jlrm.sge;

public class SGEJobStatusInfo {

    private String jobId;

    private SGEJobStatusType type;

    private String queue;

    public SGEJobStatusInfo(String jobId, SGEJobStatusType type, String queue) {
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

    public SGEJobStatusType getType() {
        return type;
    }

    public void setType(SGEJobStatusType type) {
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
