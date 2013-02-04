package org.renci.jlrm;

import java.util.Map;

public class Site {

    private String submitHost;

    private String LRMBinDirectory;

    private int maxTotalPending;

    private int maxTotalRunning;

    private long maxNoClaimTime;

    private String username;

    private String name;

    private String project;

    private Map<String, Queue> queueInfoMap;

    public Site() {
        super();
    }

    public String getLRMBinDirectory() {
        return LRMBinDirectory;
    }

    public void setLRMBinDirectory(String lRMBinDirectory) {
        LRMBinDirectory = lRMBinDirectory;
    }

    public String getSubmitHost() {
        return submitHost;
    }

    public void setSubmitHost(String submitHost) {
        this.submitHost = submitHost;
    }

    public int getMaxTotalPending() {
        return maxTotalPending;
    }

    public void setMaxTotalPending(int maxTotalPending) {
        this.maxTotalPending = maxTotalPending;
    }

    public int getMaxTotalRunning() {
        return maxTotalRunning;
    }

    public void setMaxTotalRunning(int maxTotalRunning) {
        this.maxTotalRunning = maxTotalRunning;
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

    public Map<String, Queue> getQueueInfoMap() {
        return queueInfoMap;
    }

    public void setQueueInfoMap(Map<String, Queue> queueInfoMap) {
        this.queueInfoMap = queueInfoMap;
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

    @Override
    public String toString() {
        return "Site [submitHost=" + submitHost + ", LRMBinDirectory=" + LRMBinDirectory + ", maxTotalPending="
                + maxTotalPending + ", maxTotalRunning=" + maxTotalRunning + ", maxNoClaimTime=" + maxNoClaimTime
                + ", username=" + username + ", name=" + name + ", project=" + project + ", queueInfoMap="
                + queueInfoMap + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((LRMBinDirectory == null) ? 0 : LRMBinDirectory.hashCode());
        result = prime * result + (int) (maxNoClaimTime ^ (maxNoClaimTime >>> 32));
        result = prime * result + maxTotalPending;
        result = prime * result + maxTotalRunning;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((project == null) ? 0 : project.hashCode());
        result = prime * result + ((queueInfoMap == null) ? 0 : queueInfoMap.hashCode());
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
        if (LRMBinDirectory == null) {
            if (other.LRMBinDirectory != null)
                return false;
        } else if (!LRMBinDirectory.equals(other.LRMBinDirectory))
            return false;
        if (maxNoClaimTime != other.maxNoClaimTime)
            return false;
        if (maxTotalPending != other.maxTotalPending)
            return false;
        if (maxTotalRunning != other.maxTotalRunning)
            return false;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (project == null) {
            if (other.project != null)
                return false;
        } else if (!project.equals(other.project))
            return false;
        if (queueInfoMap == null) {
            if (other.queueInfoMap != null)
                return false;
        } else if (!queueInfoMap.equals(other.queueInfoMap))
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
