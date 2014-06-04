package org.renci.jlrm.sge.ssh;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.renci.jlrm.sge.SGEJobBuilder;

public class SGESSHJobBuilder extends SGEJobBuilder {

    private boolean transferInputs = Boolean.FALSE;

    private boolean transferExecutable = Boolean.FALSE;

    private List<File> inputFiles = new ArrayList<File>();

    public SGESSHJobBuilder() {
        super();
    }

    public boolean transferInputs() {
        return transferInputs;
    }

    public SGESSHJobBuilder setTransferInputs(Boolean transferInputs) {
        this.transferInputs = transferInputs;
        return this;
    }

    public boolean transferExecutable() {
        return transferExecutable;
    }

    public SGESSHJobBuilder transferExecutable(Boolean transferExecutable) {
        this.transferExecutable = transferExecutable;
        return this;
    }

    public List<File> inputFiles() {
        return inputFiles;
    }

    public void addAllInputFiles(List<File> inputFiles) {
        inputFiles.addAll(inputFiles);
    }

    public void addInputFile(File inputFile) {
        inputFiles.add(inputFile);
    }

    public SGESSHJobBuilder id(String id) {
        this.id = id;
        return this;
    }

    public SGESSHJobBuilder name(String name) {
        this.name = name;
        return this;
    }

    public SGESSHJobBuilder executable(File executable) {
        this.executable = executable;
        return this;
    }

    public SGESSHJobBuilder submitFile(File submitFile) {
        this.submitFile = submitFile;
        return this;
    }

    public SGESSHJobBuilder output(File output) {
        this.output = output;
        return this;
    }

    public SGESSHJobBuilder error(File error) {
        this.error = error;
        return this;
    }

    public SGESSHJobBuilder numberOfProcessors(Integer numberOfProcessors) {
        this.numberOfProcessors = numberOfProcessors;
        return this;
    }

    public SGESSHJobBuilder memory(Integer memory) {
        this.memory = memory;
        return this;
    }

    public SGESSHJobBuilder duration(long duration) {
        this.duration = duration;
        return this;
    }

    public SGESSHJobBuilder durationTimeUnit(TimeUnit durationTimeUnit) {
        this.durationTimeUnit = durationTimeUnit;
        return this;
    }

    public SGESSHJobBuilder queueName(String queueName) {
        this.queueName = queueName;
        return this;
    }

    public SGESSHJobBuilder project(String project) {
        this.project = project;
        return this;
    }

    public SGESSHJobBuilder wallTime(Long wallTime) {
        this.wallTime = wallTime;
        return this;
    }

    public SGESSHJobBuilder hostCount(Integer hostCount) {
        this.hostCount = hostCount;
        return this;
    }

    public SGESSHJob build() {
        return new SGESSHJob(this);
    }

}
