package org.renci.jlrm.condor;

import static org.renci.jlrm.condor.ClassAdvertisementFactory.CLASS_AD_KEY_ARGUMENTS;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.renci.jlrm.Job;

public class CondorJob extends Job {

    private Map<String, ClassAdvertisement> classAdvertismentMap = new HashMap<String, ClassAdvertisement>();

    private int cluster;

    private int jobId;

    private Integer retry;

    private List<String> preScriptList = new ArrayList<String>();

    private List<String> postScriptList = new ArrayList<String>();

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

    public Integer getRetry() {
        return retry;
    }

    public void setRetry(Integer retry) {
        this.retry = retry;
    }

    public int getCluster() {
        return cluster;
    }

    public void setCluster(int cluster) {
        this.cluster = cluster;
    }

    public int getJobId() {
        return jobId;
    }

    public void setJobId(int jobId) {
        this.jobId = jobId;
    }

    public void addPreScript(String command) {
        this.preScriptList.add(command);
    }

    public void addPostScript(String command) {
        this.postScriptList.add(command);
    }

    public List<String> getPreScriptList() {
        return preScriptList;
    }

    public void setPreScriptList(List<String> preScriptList) {
        this.preScriptList = preScriptList;
    }

    public List<String> getPostScriptList() {
        return postScriptList;
    }

    public void setPostScriptList(List<String> postScriptList) {
        this.postScriptList = postScriptList;
    }

    @Override
    public String toString() {
        return "CondorJob [classAdvertismentMap=" + classAdvertismentMap + ", cluster=" + cluster + ", jobId=" + jobId
                + ", retry=" + retry + ", preScriptList=" + preScriptList + ", postScriptList=" + postScriptList + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((classAdvertismentMap == null) ? 0 : classAdvertismentMap.hashCode());
        result = prime * result + cluster;
        result = prime * result + jobId;
        result = prime * result + ((postScriptList == null) ? 0 : postScriptList.hashCode());
        result = prime * result + ((preScriptList == null) ? 0 : preScriptList.hashCode());
        result = prime * result + ((retry == null) ? 0 : retry.hashCode());
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
        if (classAdvertismentMap == null) {
            if (other.classAdvertismentMap != null)
                return false;
        } else if (!classAdvertismentMap.equals(other.classAdvertismentMap))
            return false;
        if (cluster != other.cluster)
            return false;
        if (jobId != other.jobId)
            return false;
        if (postScriptList == null) {
            if (other.postScriptList != null)
                return false;
        } else if (!postScriptList.equals(other.postScriptList))
            return false;
        if (preScriptList == null) {
            if (other.preScriptList != null)
                return false;
        } else if (!preScriptList.equals(other.preScriptList))
            return false;
        if (retry == null) {
            if (other.retry != null)
                return false;
        } else if (!retry.equals(other.retry))
            return false;
        return true;
    }

}
