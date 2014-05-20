package org.renci.jlrm.condor;

import static org.renci.jlrm.condor.ClassAdvertisementFactory.CLASS_AD_KEY_ARGUMENTS;
import static org.renci.jlrm.condor.ClassAdvertisementFactory.CLASS_AD_KEY_REQUIREMENTS;

import java.io.File;
import java.util.HashSet;
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

    @XmlElementWrapper(name = "classAdvertisements")
    @XmlElement(name = "classAd")
    private final Set<ClassAdvertisement> classAdvertisments = new HashSet<ClassAdvertisement>();

    private Integer cluster;

    private Integer jobId;

    private Integer retry;

    private String preScript;

    private String postScript;

    @XmlTransient
    private ClassAdvertisement argumentsClassAd;

    @XmlTransient
    private ClassAdvertisement requirementsClassAd;

    public CondorJob() {
        super();
        init();
    }

    public CondorJob(String name, File executable) {
        super(name, executable);
        init();
    }

    public CondorJob(String name, File executable, Integer retry) {
        super(name, executable);
        this.retry = retry;
        init();
        for (ClassAdvertisement classAd : ClassAdvertisementFactory.getDefaultClassAds()) {
            classAdvertisments.add(classAd);
        }
    }

    private void init() {
        this.argumentsClassAd = new ClassAdvertisement(CLASS_AD_KEY_ARGUMENTS, ClassAdvertisementType.STRING);
        this.classAdvertisments.add(this.argumentsClassAd);

        String requirements = "(Arch == \"X86_64\") && (OpSys == \"LINUX\") && (Memory >= 500) && (Disk >= 0)";
        this.requirementsClassAd = new ClassAdvertisement(CLASS_AD_KEY_REQUIREMENTS, ClassAdvertisementType.EXPRESSION,
                requirements);
        this.requirementsClassAd.setValue(requirements);
        this.classAdvertisments.add(this.requirementsClassAd);
    }

    public Set<ClassAdvertisement> getClassAdvertisments() {
        return classAdvertisments;
    }

    public void addArgument(String flag) {
        addArgument(flag, "", "");
    }

    public void addArgument(String flag, Object value) {
        addArgument(flag, value, " ");
    }

    public void addArgument(String flag, Object value, String delimiter) {
        String arg = String.format("%s%s%s", flag, delimiter, value.toString());
        String argumentsClassAdValue = this.argumentsClassAd.getValue() != null ? String.format("%s %s",
                this.argumentsClassAd.getValue(), arg) : arg;
        this.argumentsClassAd.setValue(argumentsClassAdValue);
    }

    public void addRequirement(String expression) {
        String arg = String.format("&& (%s)", expression);
        this.requirementsClassAd.setValue(String.format("%s %s", this.requirementsClassAd.getValue(), arg));
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

    public void setPriority(Integer priority) {
        ClassAdvertisement priorityClassAd = new ClassAdvertisement(ClassAdvertisementFactory.CLASS_AD_KEY_PRIORITY,
                ClassAdvertisementType.INTEGER);
        for (ClassAdvertisement classAd : getClassAdvertisments()) {
            if (classAd.equals(priorityClassAd)) {
                classAd.setValue(priority.toString());
                break;
            }
        }
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

    public void setSiteName(String siteName) {
        if (StringUtils.isNotEmpty(siteName)) {
            this.addRequirement(String.format("TARGET.JLRM_SITE_NAME == \"%s\"", siteName));
        }
    }

    public void setInitialDirectory(File initialDirectory) {
        ClassAdvertisement initialDirClassAd = new ClassAdvertisement(
                ClassAdvertisementFactory.CLASS_AD_KEY_INITIAL_DIR, ClassAdvertisementType.EXPRESSION);
        for (ClassAdvertisement classAd : getClassAdvertisments()) {
            if (classAd.equals(initialDirClassAd)) {
                classAd.setValue(initialDirectory.getAbsolutePath());
                break;
            }
        }
    }

    public void setNumberOfProcessors(Integer numberOfProcessors) {
        this.numberOfProcessors = numberOfProcessors;
        ClassAdvertisement requestCPUsClassAd = new ClassAdvertisement(
                ClassAdvertisementFactory.CLASS_AD_KEY_REQUEST_CPUS, ClassAdvertisementType.INTEGER,
                numberOfProcessors.toString());
        for (ClassAdvertisement classAd : getClassAdvertisments()) {
            if (classAd.equals(requestCPUsClassAd)) {
                classAd.setValue(numberOfProcessors.toString());
                break;
            }
        }
    }

    public void setMemory(Integer memory) {
        this.memory = memory;
        ClassAdvertisement requestMemoryClassAd = new ClassAdvertisement(
                ClassAdvertisementFactory.CLASS_AD_KEY_REQUEST_MEMORY, ClassAdvertisementType.INTEGER);
        for (ClassAdvertisement classAd : getClassAdvertisments()) {
            if (classAd.equals(requestMemoryClassAd)) {
                classAd.setValue(memory.toString());
                break;
            }
        }
    }

    @Override
    public String toString() {
        return String.format(
                "CondorJob [cluster=%s, jobId=%s, retry=%s, id=%s, name=%s, executable=%s, submitFile=%s]", cluster,
                jobId, retry, id, name, executable, submitFile);
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
