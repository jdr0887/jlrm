package org.renci.jlrm.sge;

public class SGEJobStatusInfo {

    private String jobId;

    private SGEJobStatusType type;

    private String queue;

    private String jobName;

    public SGEJobStatusInfo(String jobId, SGEJobStatusType type, String queue, String jobName) {
        super();
        this.jobId = jobId;
        this.type = type;
        this.queue = queue;
        this.jobName = jobName;
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
        return String.format("SGEJobStatusInfo [jobId=%s, type=%s, queue=%s, jobName=%s]", jobId, type, queue, jobName);
    }

}
