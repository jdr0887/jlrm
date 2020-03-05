package org.renci.jlrm.condor;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

import org.apache.commons.lang3.StringUtils;
import org.renci.jlrm.Job;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "CondorJob", propOrder = {})
@XmlRootElement(name = "condorJob")
@Getter
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class CondorJob extends Job {

    private static final long serialVersionUID = -4003065479653041431L;

    private Integer cluster;

    private Integer jobId;

    private Integer retry;

    private String preScript;

    private String postScript;

    private String siteName;

    private String initialDirectory;

    private Integer priority;

    @XmlElementWrapper(name = "classAdvertisements")
    @XmlElement(name = "classAd")
    private Set<ClassAdvertisement> classAdvertisments = new HashSet<ClassAdvertisement>();

    @XmlTransient
    private ClassAdvertisement argumentsClassAd = new ClassAdvertisement(
            ClassAdvertisementFactory.CLASS_AD_KEY_ARGUMENTS, ClassAdvertisementType.STRING);

    @XmlTransient
    private ClassAdvertisement requirementsClassAd = new ClassAdvertisement(
            ClassAdvertisementFactory.CLASS_AD_KEY_REQUIREMENTS, ClassAdvertisementType.EXPRESSION);

    @Builder
    public CondorJob(String id, String name, Path executable, Path submitFile, Path output, Path error,
            Integer numberOfProcessors, Integer memory, String disk, long duration, TimeUnit durationTimeUnit,
            Integer cluster, Integer jobId, Integer retry, String preScript, String postScript, String siteName,
            String initialDirectory, Integer priority, Set<ClassAdvertisement> classAdvertisments,
            ClassAdvertisement argumentsClassAd, ClassAdvertisement requirementsClassAd) {
        super(id, name, executable, submitFile, output, error, numberOfProcessors, memory, disk, duration,
                durationTimeUnit);
        this.cluster = cluster;
        this.jobId = jobId;
        this.retry = retry;
        this.preScript = preScript;
        this.postScript = postScript;
        this.siteName = siteName;
        this.initialDirectory = initialDirectory;
        this.priority = priority;
        this.classAdvertisments = classAdvertisments;
        this.argumentsClassAd = argumentsClassAd;
        this.requirementsClassAd = requirementsClassAd;
    }

    public CondorJob priority(Integer priority) {
        this.priority = priority;
        ClassAdvertisement priorityClassAd = new ClassAdvertisement(ClassAdvertisementFactory.CLASS_AD_KEY_PRIORITY,
                ClassAdvertisementType.INTEGER);
        if (!this.classAdvertisments.contains(priorityClassAd)) {
            priorityClassAd.setValue(priority.toString());
            this.classAdvertisments.add(priorityClassAd);
            return this;
        }
        for (ClassAdvertisement classAd : this.classAdvertisments) {
            if (classAd.equals(priorityClassAd)) {
                classAd.setValue(priority.toString());
                break;
            }
        }
        return this;
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

    public CondorJob addArgument(String flag) {
        addArgument(flag, "", "");
        return this;
    }

    public CondorJob addArgument(String flag, Object value) {
        addArgument(flag, value, " ");
        return this;
    }

    public CondorJob addArgument(String flag, Object value, String delimiter) {
        String arg = String.format("%s%s%s", flag, delimiter, value.toString());
        String argumentsClassAdValue = this.argumentsClassAd.getValue() != null
                ? String.format("%s %s", this.argumentsClassAd.getValue(), arg)
                : arg;
        this.argumentsClassAd.setValue(argumentsClassAdValue);
        return this;
    }

    public ClassAdvertisement argumentsClassAd() {
        return argumentsClassAd;
    }

    public CondorJob addRequirement(String expression) {
        String arg = String.format("&& (%s)", expression);
        String requirementsClassAdValue = this.requirementsClassAd.getValue() != null
                ? String.format("%s %s", this.requirementsClassAd.getValue(), arg)
                : arg;
        this.requirementsClassAd.setValue(requirementsClassAdValue);
        return this;
    }

    public ClassAdvertisement requirementsClassAd() {
        return requirementsClassAd;
    }

    public CondorJob addTransferInput(String file) {
        ClassAdvertisement transferInputFilesClassAd = new ClassAdvertisement(
                ClassAdvertisementFactory.CLASS_AD_KEY_TRANSFER_INPUT_FILES, ClassAdvertisementType.EXPRESSION);
        if (!this.classAdvertisments.contains(transferInputFilesClassAd)) {
            transferInputFilesClassAd.setValue(file);
            this.classAdvertisments.add(transferInputFilesClassAd);
            return this;
        }

        for (ClassAdvertisement classAd : this.classAdvertisments) {
            if (classAd.equals(transferInputFilesClassAd)) {
                classAd.setValue(String.format("%s,%s", classAd.getValue(), file));
                break;
            }
        }
        return this;
    }

    public CondorJob addTransferOutput(String file) {
        ClassAdvertisement transferOutputFilesClassAd = new ClassAdvertisement(
                ClassAdvertisementFactory.CLASS_AD_KEY_TRANSFER_OUTPUT_FILES, ClassAdvertisementType.EXPRESSION);
        if (!this.classAdvertisments.contains(transferOutputFilesClassAd)) {
            transferOutputFilesClassAd.setValue(file);
            this.classAdvertisments.add(transferOutputFilesClassAd);
            return this;
        }

        for (ClassAdvertisement classAd : this.classAdvertisments) {
            if (classAd.equals(transferOutputFilesClassAd)) {
                classAd.setValue(String.format("%s,%s", classAd.getValue(), file));
                break;
            }
        }
        return this;
    }

    public CondorJob cluster(Integer cluster) {
        this.cluster = cluster;
        return this;
    }

    public CondorJob siteName(String siteName) {
        this.siteName = siteName;
        if (StringUtils.isNotEmpty(siteName)) {
            this.addRequirement(String.format("TARGET.JLRM_SITE_NAME == \"%s\"", siteName));
        }
        return this;
    }

    public CondorJob initialDirectory(String initialDirectory) {
        this.initialDirectory = initialDirectory;
        ClassAdvertisement initialDirClassAd = new ClassAdvertisement(
                ClassAdvertisementFactory.CLASS_AD_KEY_INITIAL_DIR, ClassAdvertisementType.EXPRESSION);
        if (!this.classAdvertisments.contains(initialDirClassAd)) {
            initialDirClassAd.setValue(initialDirectory);
            this.classAdvertisments.add(initialDirClassAd);
            return this;
        }
        for (ClassAdvertisement classAd : this.classAdvertisments) {
            if (classAd.equals(initialDirClassAd)) {
                classAd.setValue(initialDirectory);
                break;
            }
        }
        return this;
    }

    public CondorJob numberOfProcessors(Integer numberOfProcessors) {
        this.numberOfProcessors = numberOfProcessors;
        ClassAdvertisement requestCPUsClassAd = new ClassAdvertisement(
                ClassAdvertisementFactory.CLASS_AD_KEY_REQUEST_CPUS, ClassAdvertisementType.INTEGER,
                numberOfProcessors.toString());
        for (ClassAdvertisement classAd : this.classAdvertisments) {
            if (classAd.equals(requestCPUsClassAd)) {
                classAd.setValue(numberOfProcessors.toString());
                break;
            }
        }
        return this;
    }

    public CondorJob memory(Integer memory) {
        this.memory = memory;
        ClassAdvertisement requestMemoryClassAd = new ClassAdvertisement(
                ClassAdvertisementFactory.CLASS_AD_KEY_REQUEST_MEMORY, ClassAdvertisementType.INTEGER);
        for (ClassAdvertisement classAd : this.classAdvertisments) {
            if (classAd.equals(requestMemoryClassAd)) {
                classAd.setValue(String.format("%sGB", memory.toString()));
                break;
            }
        }
        return this;
    }

}
