package org.renci.jlrm.pbs.ssh;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.renci.jlrm.pbs.PBSJobBuilder;

public class PBSSSHJobBuilder extends PBSJobBuilder {

    private boolean transferInputs = Boolean.FALSE;

    private boolean transferExecutable = Boolean.FALSE;

    private List<File> inputFiles = new ArrayList<File>();

    public PBSSSHJobBuilder() {
        super();
    }

    public boolean transferInputs() {
        return transferInputs;
    }

    public PBSSSHJobBuilder setTransferInputs(Boolean transferInputs) {
        this.transferInputs = transferInputs;
        return this;
    }

    public boolean transferExecutable() {
        return transferExecutable;
    }

    public PBSSSHJobBuilder transferExecutable(Boolean transferExecutable) {
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

    public PBSSSHJobBuilder id(String id) {
        this.id = id;
        return this;
    }

    public PBSSSHJobBuilder name(String name) {
        this.name = name;
        return this;
    }

    public PBSSSHJobBuilder executable(File executable) {
        this.executable = executable;
        return this;
    }

    public PBSSSHJobBuilder submitFile(File submitFile) {
        this.submitFile = submitFile;
        return this;
    }

    public PBSSSHJobBuilder output(File output) {
        this.output = output;
        return this;
    }

    public PBSSSHJobBuilder error(File error) {
        this.error = error;
        return this;
    }

    public PBSSSHJobBuilder numberOfProcessors(Integer numberOfProcessors) {
        this.numberOfProcessors = numberOfProcessors;
        return this;
    }

    public PBSSSHJobBuilder memory(String memory) {
        this.memory = memory;
        return this;
    }

    public PBSSSHJobBuilder duration(long duration) {
        this.duration = duration;
        return this;
    }

    public PBSSSHJobBuilder durationTimeUnit(TimeUnit durationTimeUnit) {
        this.durationTimeUnit = durationTimeUnit;
        return this;
    }

    public PBSSSHJobBuilder queueName(String queueName) {
        this.queueName = queueName;
        return this;
    }

    public PBSSSHJobBuilder project(String project) {
        this.project = project;
        return this;
    }

    public PBSSSHJobBuilder wallTime(Long wallTime) {
        this.wallTime = wallTime;
        return this;
    }

    public PBSSSHJobBuilder hostCount(Integer hostCount) {
        this.hostCount = hostCount;
        return this;
    }

    public PBSSSHJob build() {
        return new PBSSSHJob(this);
    }

}
