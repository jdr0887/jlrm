package org.renci.jlrm.lsf.ssh;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.renci.jlrm.lsf.LSFJobBuilder;

public class LSFSSHJobBuilder extends LSFJobBuilder {

    private boolean transferInputs = Boolean.FALSE;

    private boolean transferExecutable = Boolean.FALSE;

    private List<File> inputFiles = new ArrayList<File>();

    public LSFSSHJobBuilder() {
        super();
    }

    public boolean transferInputs() {
        return transferInputs;
    }

    public LSFSSHJobBuilder setTransferInputs(Boolean transferInputs) {
        this.transferInputs = transferInputs;
        return this;
    }

    public boolean transferExecutable() {
        return transferExecutable;
    }

    public LSFSSHJobBuilder transferExecutable(Boolean transferExecutable) {
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

    public LSFSSHJobBuilder id(String id) {
        this.id = id;
        return this;
    }

    public LSFSSHJobBuilder name(String name) {
        this.name = name;
        return this;
    }

    public LSFSSHJobBuilder executable(File executable) {
        this.executable = executable;
        return this;
    }

    public LSFSSHJobBuilder submitFile(File submitFile) {
        this.submitFile = submitFile;
        return this;
    }

    public LSFSSHJobBuilder output(File output) {
        this.output = output;
        return this;
    }

    public LSFSSHJobBuilder error(File error) {
        this.error = error;
        return this;
    }

    public LSFSSHJobBuilder numberOfProcessors(Integer numberOfProcessors) {
        this.numberOfProcessors = numberOfProcessors;
        return this;
    }

    public LSFSSHJobBuilder memory(String memory) {
        this.memory = memory;
        return this;
    }

    public LSFSSHJobBuilder duration(long duration) {
        this.duration = duration;
        return this;
    }

    public LSFSSHJobBuilder durationTimeUnit(TimeUnit durationTimeUnit) {
        this.durationTimeUnit = durationTimeUnit;
        return this;
    }

    public LSFSSHJobBuilder queueName(String queueName) {
        this.queueName = queueName;
        return this;
    }

    public LSFSSHJobBuilder project(String project) {
        this.project = project;
        return this;
    }

    public LSFSSHJobBuilder wallTime(Long wallTime) {
        this.wallTime = wallTime;
        return this;
    }

    public LSFSSHJobBuilder hostCount(Integer hostCount) {
        this.hostCount = hostCount;
        return this;
    }

    public LSFSSHJob build() {
        return new LSFSSHJob(this);
    }

}
