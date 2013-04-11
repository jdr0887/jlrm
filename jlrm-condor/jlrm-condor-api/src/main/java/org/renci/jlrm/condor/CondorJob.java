package org.renci.jlrm.condor;

import static org.renci.jlrm.condor.ClassAdvertisementFactory.CLASS_AD_KEY_ARGUMENTS;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.renci.jlrm.Job;

public class CondorJob extends Job {

    private Map<String, ClassAdvertisement> classAdvertismentMap = new HashMap<String, ClassAdvertisement>();

    private Integer cluster;

    private Integer jobId;

    private Integer priority;

    private Integer retry;

    private String preScript;

    private String postScript;

    private String siteName;

    public CondorJob() {
        super();
    }

    public CondorJob(String name, File executable) {
        super();
        this.name = name;
        this.executable = executable;
    }

    public CondorJob(String name, File executable, Integer retry) {
        super();
        this.name = name;
        this.executable = executable;
        this.retry = retry;
        for (ClassAdvertisement classAd : ClassAdvertisementFactory.getDefaultClassAds()) {
            classAdvertismentMap.put(classAd.getKey(), classAd);
        }
    }

    public Map<String, ClassAdvertisement> getClassAdvertismentMap() {
        return classAdvertismentMap;
    }

    public void setClassAdvertismentMap(Map<String, ClassAdvertisement> classAdvertismentMap) {
        this.classAdvertismentMap = classAdvertismentMap;
    }

    public void addArgument(String flag) {
        addArgument(flag, "", "");
    }

    public void addArgument(String flag, Object value) {
        addArgument(flag, value, " ");
    }

    public void addArgument(String flag, Object value, String delimiter) {
        try {
            if (!getClassAdvertismentMap().containsKey(CLASS_AD_KEY_ARGUMENTS)) {
                getClassAdvertismentMap().put(CLASS_AD_KEY_ARGUMENTS,
                        ClassAdvertisementFactory.getClassAd(CLASS_AD_KEY_ARGUMENTS).clone());
            }
            String arg = String.format("%s%s%s", flag, delimiter, value.toString());
            ClassAdvertisement classAd = getClassAdvertismentMap().get(CLASS_AD_KEY_ARGUMENTS);
            classAd.setValue(classAd.getValue() + " " + arg);
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
    }

    public Integer getCluster() {
        return cluster;
    }

    public void setCluster(Integer cluster) {
        this.cluster = cluster;
    }

    public Integer getJobId() {
        return jobId;
    }

    public void setJobId(Integer jobId) {
        this.jobId = jobId;
    }

    public Integer getPriority() {
        return priority;
    }

    public void setPriority(Integer priority) {
        this.priority = priority;
    }

    public Integer getRetry() {
        return retry;
    }

    public void setRetry(Integer retry) {
        this.retry = retry;
    }

    public String getPreScript() {
        return preScript;
    }

    public void setPreScript(String preScript) {
        this.preScript = preScript;
    }

    public String getPostScript() {
        return postScript;
    }

    public void setPostScript(String postScript) {
        this.postScript = postScript;
    }

    public String getSiteName() {
        return siteName;
    }

    public void setSiteName(String siteName) {
        this.siteName = siteName;
    }

    @Override
    public String toString() {
        return String.format(
                "CondorJob [cluster=%s, jobId=%s, priority=%s, retry=%s, preScript=%s, postScript=%s, siteName=%s]",
                cluster, jobId, priority, retry, preScript, postScript, siteName);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((cluster == null) ? 0 : cluster.hashCode());
        result = prime * result + ((jobId == null) ? 0 : jobId.hashCode());
        result = prime * result + ((postScript == null) ? 0 : postScript.hashCode());
        result = prime * result + ((preScript == null) ? 0 : preScript.hashCode());
        result = prime * result + ((priority == null) ? 0 : priority.hashCode());
        result = prime * result + ((retry == null) ? 0 : retry.hashCode());
        result = prime * result + ((siteName == null) ? 0 : siteName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        CondorJob other = (CondorJob) obj;
        if (cluster == null) {
            if (other.cluster != null)
                return false;
        } else if (!cluster.equals(other.cluster))
            return false;
        if (jobId == null) {
            if (other.jobId != null)
                return false;
        } else if (!jobId.equals(other.jobId))
            return false;
        if (postScript == null) {
            if (other.postScript != null)
                return false;
        } else if (!postScript.equals(other.postScript))
            return false;
        if (preScript == null) {
            if (other.preScript != null)
                return false;
        } else if (!preScript.equals(other.preScript))
            return false;
        if (priority == null) {
            if (other.priority != null)
                return false;
        } else if (!priority.equals(other.priority))
            return false;
        if (retry == null) {
            if (other.retry != null)
                return false;
        } else if (!retry.equals(other.retry))
            return false;
        if (siteName == null) {
            if (other.siteName != null)
                return false;
        } else if (!siteName.equals(other.siteName))
            return false;
        return true;
    }

}
