package org.renci.jlrm.condor;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.renci.jlrm.JobBuilder;

public class CondorJobBuilder extends JobBuilder {

    private final Set<ClassAdvertisement> classAdvertisments = new HashSet<ClassAdvertisement>();

    private Integer cluster;

    private Integer jobId;

    private Integer retry;

    private String preScript;

    private String postScript;

    private String siteName;

    private String initialDirectory;

    private Integer priority;

    private ClassAdvertisement argumentsClassAd = new ClassAdvertisement(
            ClassAdvertisementFactory.CLASS_AD_KEY_ARGUMENTS, ClassAdvertisementType.STRING);

    private ClassAdvertisement requirementsClassAd = new ClassAdvertisement(
            ClassAdvertisementFactory.CLASS_AD_KEY_REQUIREMENTS, ClassAdvertisementType.EXPRESSION);

    public CondorJobBuilder() {
        super();
    }

    public CondorJobBuilder id(String id) {
        this.id = id;
        return this;
    }

    public CondorJobBuilder name(String name) {
        this.name = name;
        return this;
    }

    public CondorJobBuilder executable(File executable) {
        this.executable = executable;
        return this;
    }

    public CondorJobBuilder submitFile(File submitFile) {
        this.submitFile = submitFile;
        return this;
    }

    public CondorJobBuilder output(File output) {
        this.output = output;
        return this;
    }

    public CondorJobBuilder error(File error) {
        this.error = error;
        return this;
    }

    public CondorJobBuilder duration(long duration) {
        this.duration = duration;
        return this;
    }

    public CondorJobBuilder durationTimeUnit(TimeUnit durationTimeUnit) {
        this.durationTimeUnit = durationTimeUnit;
        return this;
    }

    public Set<ClassAdvertisement> classAdvertisments() {
        return classAdvertisments;
    }

    public CondorJobBuilder addArgument(String flag) {
        addArgument(flag, "", "");
        return this;
    }

    public CondorJobBuilder addArgument(String flag, Object value) {
        addArgument(flag, value, " ");
        return this;
    }

    public CondorJobBuilder addArgument(String flag, Object value, String delimiter) {
        String arg = String.format("%s%s%s", flag, delimiter, value.toString());
        String argumentsClassAdValue = this.argumentsClassAd.getValue() != null ? String.format("%s %s",
                this.argumentsClassAd.getValue(), arg) : arg;
        this.argumentsClassAd.setValue(argumentsClassAdValue);
        return this;
    }

    public ClassAdvertisement argumentsClassAd() {
        return argumentsClassAd;
    }

    public CondorJobBuilder addRequirement(String expression) {
        String arg = String.format("&& (%s)", expression);
        String requirementsClassAdValue = this.requirementsClassAd.getValue() != null ? String.format("%s %s",
                this.requirementsClassAd.getValue(), arg) : arg;
        this.requirementsClassAd.setValue(requirementsClassAdValue);
        return this;
    }

    public ClassAdvertisement requirementsClassAd() {
        return requirementsClassAd;
    }

    public CondorJobBuilder addTransferInput(String file) {
        ClassAdvertisement transferInputFilesClassAd = new ClassAdvertisement(
                ClassAdvertisementFactory.CLASS_AD_KEY_TRANSFER_INPUT_FILES, ClassAdvertisementType.EXPRESSION);
        if (!classAdvertisments().contains(transferInputFilesClassAd)) {
            transferInputFilesClassAd.setValue(file);
            this.classAdvertisments.add(transferInputFilesClassAd);
            return this;
        }

        for (ClassAdvertisement classAd : classAdvertisments()) {
            if (classAd.equals(transferInputFilesClassAd)) {
                classAd.setValue(String.format("%s,%s", classAd.getValue(), file));
                break;
            }
        }
        return this;
    }

    public CondorJobBuilder addTransferOutput(String file) {
        ClassAdvertisement transferOutputFilesClassAd = new ClassAdvertisement(
                ClassAdvertisementFactory.CLASS_AD_KEY_TRANSFER_OUTPUT_FILES, ClassAdvertisementType.EXPRESSION);
        if (!classAdvertisments().contains(transferOutputFilesClassAd)) {
            transferOutputFilesClassAd.setValue(file);
            this.classAdvertisments.add(transferOutputFilesClassAd);
            return this;
        }

        for (ClassAdvertisement classAd : classAdvertisments()) {
            if (classAd.equals(transferOutputFilesClassAd)) {
                classAd.setValue(String.format("%s,%s", classAd.getValue(), file));
                break;
            }
        }
        return this;
    }

    public Integer cluster() {
        return cluster;
    }

    public CondorJobBuilder cluster(Integer cluster) {
        this.cluster = cluster;
        return this;
    }

    public Integer jobId() {
        return jobId;
    }

    public CondorJobBuilder jobId(Integer jobId) {
        this.jobId = jobId;
        return this;
    }

    public Integer priority() {
        return priority;
    }

    public CondorJobBuilder priority(Integer priority) {
        this.priority = priority;
        ClassAdvertisement priorityClassAd = new ClassAdvertisement(ClassAdvertisementFactory.CLASS_AD_KEY_PRIORITY,
                ClassAdvertisementType.INTEGER);
        for (ClassAdvertisement classAd : classAdvertisments()) {
            if (classAd.equals(priorityClassAd)) {
                classAd.setValue(priority.toString());
                break;
            }
        }
        return this;
    }

    public Integer retry() {
        return retry;
    }

    public CondorJobBuilder retry(Integer retry) {
        this.retry = retry;
        return this;
    }

    public String preScript() {
        return preScript;
    }

    public CondorJobBuilder preScript(String preScript) {
        this.preScript = preScript;
        return this;
    }

    public String postScript() {
        return postScript;
    }

    public CondorJobBuilder postScript(String postScript) {
        this.postScript = postScript;
        return this;
    }

    public CondorJobBuilder siteName(String siteName) {
        this.siteName = siteName;
        if (StringUtils.isNotEmpty(siteName)) {
            this.addRequirement(String.format("TARGET.JLRM_SITE_NAME == \"%s\"", siteName));
        }
        return this;
    }

    public String siteName() {
        return siteName;
    }

    public String initialDirectory() {
        return initialDirectory;
    }

    public CondorJobBuilder initialDirectory(String initialDirectory) {
        this.initialDirectory = initialDirectory;
        ClassAdvertisement initialDirClassAd = new ClassAdvertisement(
                ClassAdvertisementFactory.CLASS_AD_KEY_INITIAL_DIR, ClassAdvertisementType.EXPRESSION);
        if (!classAdvertisments().contains(initialDirClassAd)) {
            initialDirClassAd.setValue(initialDirectory);
            this.classAdvertisments.add(initialDirClassAd);
            return this;
        }
        for (ClassAdvertisement classAd : classAdvertisments()) {
            if (classAd.equals(initialDirClassAd)) {
                classAd.setValue(initialDirectory);
                break;
            }
        }
        return this;
    }

    public CondorJobBuilder numberOfProcessors(Integer numberOfProcessors) {
        this.numberOfProcessors = numberOfProcessors;
        ClassAdvertisement requestCPUsClassAd = new ClassAdvertisement(
                ClassAdvertisementFactory.CLASS_AD_KEY_REQUEST_CPUS, ClassAdvertisementType.INTEGER,
                numberOfProcessors.toString());
        for (ClassAdvertisement classAd : classAdvertisments()) {
            if (classAd.equals(requestCPUsClassAd)) {
                classAd.setValue(numberOfProcessors.toString());
                break;
            }
        }
        return this;
    }

    public CondorJobBuilder memory(Integer memory) {
        this.memory = memory;
        ClassAdvertisement requestMemoryClassAd = new ClassAdvertisement(
                ClassAdvertisementFactory.CLASS_AD_KEY_REQUEST_MEMORY, ClassAdvertisementType.INTEGER);
        for (ClassAdvertisement classAd : classAdvertisments()) {
            if (classAd.equals(requestMemoryClassAd)) {
                classAd.setValue(memory.toString());
                break;
            }
        }
        return this;
    }

    public CondorJob build() {
        return new CondorJob(this);
    }

}
