package org.renci.jlrm.pbs;

public class PBSJobStatusInfo {

    private String jobId;

    private PBSJobStatusType type;

    private String queue;

    private String jobName;

    public PBSJobStatusInfo() {
        super();
    }

    public PBSJobStatusInfo(String jobId, PBSJobStatusType type, String queue, String jobName) {
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

    public PBSJobStatusType getType() {
        return type;
    }

    public void setType(PBSJobStatusType type) {
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
        return String.format("PBSJobStatusInfo [jobId=%s, type=%s, queue=%s, jobName=%s]", jobId, type, queue, jobName);
    }

}
