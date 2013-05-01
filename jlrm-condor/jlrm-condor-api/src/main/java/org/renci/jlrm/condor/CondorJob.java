package org.renci.jlrm.condor;

import static org.renci.jlrm.condor.ClassAdvertisementFactory.CLASS_AD_KEY_ARGUMENTS;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.renci.jlrm.Job;

public class CondorJob extends Job {

    private final Map<String, ClassAdvertisement> classAdvertismentMap = new HashMap<String, ClassAdvertisement>();

    private Integer cluster;

    private Integer jobId;

    private Integer priority;

    private Integer retry;

    private String preScript;

    private String postScript;

    private String siteName;

    private File initialDirectory;

    private final List<String> transferInputList = new ArrayList<String>();

    private final List<String> transferOutputList = new ArrayList<String>();

    public CondorJob() {
        super();
    }

    public CondorJob(String name, File executable) {
        super(name, executable);
    }

    public CondorJob(String name, File executable, Integer retry) {
        super(name, executable);
        this.retry = retry;
        for (ClassAdvertisement classAd : ClassAdvertisementFactory.getDefaultClassAds()) {
            classAdvertismentMap.put(classAd.getKey(), classAd);
        }
    }

    public Map<String, ClassAdvertisement> getClassAdvertismentMap() {
        return classAdvertismentMap;
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

    public void addTransferInput(File file) {
        this.transferInputList.add(file.getAbsolutePath());
    }

    public void addTransferInput(String file) {
        this.transferInputList.add(file);
    }

    public void addTransferOutput(File file) {
        this.transferOutputList.add(file.getAbsolutePath());
    }

    public void addTransferOutput(String file) {
        this.transferOutputList.add(file);
    }

    public File getInitialDirectory() {
        return initialDirectory;
    }

    public void setInitialDirectory(File initialDirectory) {
        this.initialDirectory = initialDirectory;
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

    public List<String> getTransferInputList() {
        return transferInputList;
    }

    public List<String> getTransferOutputList() {
        return transferOutputList;
    }

    @Override
    public String toString() {
        return String
                .format("CondorJob [cluster=%s, jobId=%s, priority=%s, retry=%s, siteName=%s, initialDirectory=%s, id=%s, name=%s, executable=%s, submitFile=%s]",
                        cluster, jobId, priority, retry, siteName, initialDirectory, id, name, executable, submitFile);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((cluster == null) ? 0 : cluster.hashCode());
        result = prime * result + ((initialDirectory == null) ? 0 : initialDirectory.hashCode());
        result = prime * result + ((jobId == null) ? 0 : jobId.hashCode());
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
        if (initialDirectory == null) {
            if (other.initialDirectory != null)
                return false;
        } else if (!initialDirectory.equals(other.initialDirectory))
            return false;
        if (jobId == null) {
            if (other.jobId != null)
                return false;
        } else if (!jobId.equals(other.jobId))
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
