package org.renci.jlrm.lsf;

public class LSFJobStatusInfo {

    private String jobId;

    private LSFJobStatusType type;

    private String queue;

    private String jobName;

    public LSFJobStatusInfo() {
        super();
    }

    public LSFJobStatusInfo(String jobId, LSFJobStatusType type, String queue, String jobName) {
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

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    @Override
    public String toString() {
        return "LSFJobStatusInfo [jobId=" + jobId + ", type=" + type + ", queue=" + queue + ", jobName=" + jobName
                + "]";
    }

}
