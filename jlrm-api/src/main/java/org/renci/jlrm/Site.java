package org.renci.jlrm;

import java.util.List;

public class Site {

    private String submitHost;

    private int maxPending;

    private int maxRunning;

    private long maxNoClaimTime;

    private String username;

    private String name;

    private String project;

    private Integer numberOfProcessors;

    private List<Queue> queueList;

    public Site() {
        super();
    }

    public String getSubmitHost() {
        return submitHost;
    }

    public void setSubmitHost(String submitHost) {
        this.submitHost = submitHost;
    }

    public int getMaxPending() {
        return maxPending;
    }

    public void setMaxPending(int maxPending) {
        this.maxPending = maxPending;
    }

    public int getMaxRunning() {
        return maxRunning;
    }

    public void setMaxRunning(int maxRunning) {
        this.maxRunning = maxRunning;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public List<Queue> getQueueList() {
        return queueList;
    }

    public void setQueueList(List<Queue> queueList) {
        this.queueList = queueList;
    }

    public long getMaxNoClaimTime() {
        return maxNoClaimTime;
    }

    public void setMaxNoClaimTime(long maxNoClaimTime) {
        this.maxNoClaimTime = maxNoClaimTime;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public Integer getNumberOfProcessors() {
        return numberOfProcessors;
    }

    public void setNumberOfProcessors(Integer numberOfProcessors) {
        this.numberOfProcessors = numberOfProcessors;
    }

    @Override
    public String toString() {
        return String
                .format("Site [submitHost=%s, maxPending=%s, maxRunning=%s, maxNoClaimTime=%s, username=%s, name=%s, project=%s, numberOfProcessors=%s]",
                        submitHost, maxPending, maxRunning, maxNoClaimTime, username, name, project, numberOfProcessors);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (maxNoClaimTime ^ (maxNoClaimTime >>> 32));
        result = prime * result + maxPending;
        result = prime * result + maxRunning;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((numberOfProcessors == null) ? 0 : numberOfProcessors.hashCode());
        result = prime * result + ((project == null) ? 0 : project.hashCode());
        result = prime * result + ((submitHost == null) ? 0 : submitHost.hashCode());
        result = prime * result + ((username == null) ? 0 : username.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Site other = (Site) obj;
        if (maxNoClaimTime != other.maxNoClaimTime)
            return false;
        if (maxPending != other.maxPending)
            return false;
        if (maxRunning != other.maxRunning)
            return false;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (numberOfProcessors == null) {
            if (other.numberOfProcessors != null)
                return false;
        } else if (!numberOfProcessors.equals(other.numberOfProcessors))
            return false;
        if (project == null) {
            if (other.project != null)
                return false;
        } else if (!project.equals(other.project))
            return false;
        if (submitHost == null) {
            if (other.submitHost != null)
                return false;
        } else if (!submitHost.equals(other.submitHost))
            return false;
        if (username == null) {
            if (other.username != null)
                return false;
        } else if (!username.equals(other.username))
            return false;
        return true;
    }

}
