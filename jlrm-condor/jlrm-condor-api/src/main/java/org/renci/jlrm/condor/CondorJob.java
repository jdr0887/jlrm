package org.renci.jlrm.condor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

import org.apache.commons.lang3.StringUtils;
import org.renci.jlrm.Job;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "CondorJob", propOrder = {})
@XmlRootElement(name = "condorJob")
public class CondorJob extends Job {

    private static final long serialVersionUID = -4003065479653041431L;

    @XmlElementWrapper(name = "classAdvertisements")
    @XmlElement(name = "classAd")
    private Set<ClassAdvertisement> classAdvertisments = new HashSet<ClassAdvertisement>();

    private Integer cluster;

    private Integer jobId;

    private Integer retry;

    private String preScript;

    private String postScript;

    private String siteName;

    private String initialDirectory;

    private Integer priority;

    @XmlTransient
    private ClassAdvertisement argumentsClassAd = new ClassAdvertisement(
            ClassAdvertisementFactory.CLASS_AD_KEY_ARGUMENTS, ClassAdvertisementType.STRING);

    @XmlTransient
    private ClassAdvertisement requirementsClassAd = new ClassAdvertisement(
            ClassAdvertisementFactory.CLASS_AD_KEY_REQUIREMENTS, ClassAdvertisementType.EXPRESSION);

    public CondorJob() {
        super();
    }

    public CondorJob(CondorJobBuilder builder) {
        super();
        // from JobBuilder
        this.id = builder.id();
        this.name = builder.name();
        this.executable = builder.executable();
        this.submitFile = builder.submitFile();
        this.output = builder.output();
        this.error = builder.error();
        this.numberOfProcessors = builder.numberOfProcessors();
        this.memory = builder.memory();
        this.disk = builder.disk();
        this.duration = builder.duration();
        this.durationTimeUnit = builder.durationTimeUnit();
        // from CondorJobBuilder
        this.classAdvertisments = builder.classAdvertisments();
        this.cluster = builder.cluster();
        this.jobId = builder.jobId();
        this.retry = builder.retry();
        this.preScript = builder.preScript();
        this.postScript = builder.postScript();
        this.siteName = builder.siteName();
        this.initialDirectory = builder.initialDirectory();
        this.priority = builder.priority();
        this.argumentsClassAd = builder.argumentsClassAd();
        this.requirementsClassAd = builder.requirementsClassAd();
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

    public String getInitialDirectory() {
        return initialDirectory;
    }

    public void setInitialDirectory(String initialDirectory) {
        this.initialDirectory = initialDirectory;
    }

    public Integer getPriority() {
        return priority;
    }

    public void setPriority(Integer priority) {
        this.priority = priority;
        ClassAdvertisement priorityClassAd = new ClassAdvertisement(ClassAdvertisementFactory.CLASS_AD_KEY_PRIORITY,
                ClassAdvertisementType.INTEGER);
        if (!getClassAdvertisments().contains(priorityClassAd)) {
            priorityClassAd.setValue(priority.toString());
            this.classAdvertisments.add(priorityClassAd);
            return;
        }
        for (ClassAdvertisement classAd : getClassAdvertisments()) {
            if (classAd.equals(priorityClassAd)) {
                classAd.setValue(priority.toString());
                break;
            }
        }
    }

    public ClassAdvertisement getArgumentsClassAd() {
        return argumentsClassAd;
    }

    public void setArgumentsClassAd(ClassAdvertisement argumentsClassAd) {
        this.argumentsClassAd = argumentsClassAd;
    }

    public ClassAdvertisement getRequirementsClassAd() {
        return requirementsClassAd;
    }

    public void setRequirementsClassAd(ClassAdvertisement requirementsClassAd) {
        this.requirementsClassAd = requirementsClassAd;
    }

    public Set<ClassAdvertisement> getClassAdvertisments() {
        return classAdvertisments;
    }

    public void addArgument(String flag, Object value, String delimiter) {
        String arg = String.format("%s%s%s", flag, delimiter, value.toString());
        String argumentsClassAdValue = this.argumentsClassAd.getValue() != null
                ? String.format("%s %s", this.argumentsClassAd.getValue(), arg) : arg;
        this.argumentsClassAd.setValue(argumentsClassAdValue);
    }

    public void addRequirement(String expression) {
        String arg = String.format("&& (%s)", expression);
        String requirementsClassAdValue = this.requirementsClassAd.getValue() != null
                ? String.format("%s %s", this.requirementsClassAd.getValue(), arg) : arg;
        this.requirementsClassAd.setValue(requirementsClassAdValue);
    }

    public void addTransferInput(String file) {
        ClassAdvertisement transferInputFilesClassAd = new ClassAdvertisement(
                ClassAdvertisementFactory.CLASS_AD_KEY_TRANSFER_INPUT_FILES, ClassAdvertisementType.EXPRESSION);
        if (!getClassAdvertisments().contains(transferInputFilesClassAd)) {
            transferInputFilesClassAd.setValue(file);
            this.classAdvertisments.add(transferInputFilesClassAd);
            return;
        }

        for (ClassAdvertisement classAd : getClassAdvertisments()) {
            if (classAd.equals(transferInputFilesClassAd)) {
                classAd.setValue(String.format("%s,%s", classAd.getValue(), file));
                break;
            }
        }
    }

    public void addTransferOutput(String file) {
        ClassAdvertisement transferOutputFilesClassAd = new ClassAdvertisement(
                ClassAdvertisementFactory.CLASS_AD_KEY_TRANSFER_OUTPUT_FILES, ClassAdvertisementType.EXPRESSION);
        if (!getClassAdvertisments().contains(transferOutputFilesClassAd)) {
            transferOutputFilesClassAd.setValue(file);
            this.classAdvertisments.add(transferOutputFilesClassAd);
            return;
        }

        for (ClassAdvertisement classAd : getClassAdvertisments()) {
            if (classAd.equals(transferOutputFilesClassAd)) {
                classAd.setValue(String.format("%s,%s", classAd.getValue(), file));
                break;
            }
        }
    }

    public List<String> getTransferInputList() {
        List<String> ret = new ArrayList<String>();
        ClassAdvertisement transferFilesClassAd = new ClassAdvertisement(
                ClassAdvertisementFactory.CLASS_AD_KEY_TRANSFER_INPUT_FILES, ClassAdvertisementType.EXPRESSION);
        if (getClassAdvertisments().contains(transferFilesClassAd)) {
            for (ClassAdvertisement classAd : getClassAdvertisments()) {
                if (classAd.equals(transferFilesClassAd)) {
                    ret.addAll(Arrays.asList(StringUtils.split(classAd.getValue(), ',')));
                    break;
                }
            }
        }
        return ret;
    }

    public List<String> getTransferOutputList() {
        List<String> ret = new ArrayList<String>();
        ClassAdvertisement transferFilesClassAd = new ClassAdvertisement(
                ClassAdvertisementFactory.CLASS_AD_KEY_TRANSFER_OUTPUT_FILES, ClassAdvertisementType.EXPRESSION);
        if (getClassAdvertisments().contains(transferFilesClassAd)) {
            for (ClassAdvertisement classAd : getClassAdvertisments()) {
                if (classAd.equals(transferFilesClassAd)) {
                    ret.addAll(Arrays.asList(StringUtils.split(classAd.getValue(), ',')));
                    break;
                }
            }
        }
        return ret;
    }

    @Override
    public String toString() {
        return String.format(
                "CondorJob [cluster=%s, jobId=%s, retry=%s, preScript=%s, postScript=%s, siteName=%s, initialDirectory=%s, priority=%s, argumentsClassAd=%s, requirementsClassAd=%s, id=%s, name=%s, executable=%s, submitFile=%s, output=%s, error=%s, numberOfProcessors=%s, memory=%s, duration=%s, durationTimeUnit=%s]",
                cluster, jobId, retry, preScript, postScript, siteName, initialDirectory, priority, argumentsClassAd,
                requirementsClassAd, id, name, executable, submitFile, output, error, numberOfProcessors, memory,
                duration, durationTimeUnit);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((cluster == null) ? 0 : cluster.hashCode());
        result = prime * result + ((jobId == null) ? 0 : jobId.hashCode());
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
        if (retry == null) {
            if (other.retry != null)
                return false;
        } else if (!retry.equals(other.retry))
            return false;
        return true;
    }

}
