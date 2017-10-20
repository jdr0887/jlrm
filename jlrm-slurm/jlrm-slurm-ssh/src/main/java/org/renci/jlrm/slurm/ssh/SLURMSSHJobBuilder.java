package org.renci.jlrm.slurm.ssh;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.Range;
import org.renci.jlrm.slurm.SLURMJobBuilder;

public class SLURMSSHJobBuilder extends SLURMJobBuilder {

    private Boolean transferInputs = Boolean.FALSE;

    private Boolean transferExecutable = Boolean.FALSE;

    private List<File> inputFiles = new ArrayList<File>();

    public SLURMSSHJobBuilder() {
        super();
    }

    public boolean transferInputs() {
        return transferInputs;
    }

    public SLURMSSHJobBuilder transferInputs(Boolean transferInputs) {
        this.transferInputs = transferInputs;
        return this;
    }

    public boolean transferExecutable() {
        return transferExecutable;
    }

    public SLURMSSHJobBuilder transferExecutable(Boolean transferExecutable) {
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

    public SLURMSSHJobBuilder array(Range<Integer> array) {
        this.array = array;
        return this;
    }

    public SLURMSSHJobBuilder id(String id) {
        this.id = id;
        return this;
    }

    public SLURMSSHJobBuilder name(String name) {
        this.name = name;
        return this;
    }

    public SLURMSSHJobBuilder executable(File executable) {
        this.executable = executable;
        return this;
    }

    public SLURMSSHJobBuilder submitFile(File submitFile) {
        this.submitFile = submitFile;
        return this;
    }

    public SLURMSSHJobBuilder output(File output) {
        this.output = output;
        return this;
    }

    public SLURMSSHJobBuilder error(File error) {
        this.error = error;
        return this;
    }

    public SLURMSSHJobBuilder numberOfProcessors(Integer numberOfProcessors) {
        this.numberOfProcessors = numberOfProcessors;
        return this;
    }

    public SLURMSSHJobBuilder memory(String memory) {
        this.memory = memory;
        return this;
    }

    public SLURMSSHJobBuilder duration(long duration) {
        this.duration = duration;
        return this;
    }

    public SLURMSSHJobBuilder durationTimeUnit(TimeUnit durationTimeUnit) {
        this.durationTimeUnit = durationTimeUnit;
        return this;
    }

    public SLURMSSHJobBuilder queueName(String queueName) {
        this.queueName = queueName;
        return this;
    }

    public SLURMSSHJobBuilder project(String project) {
        this.project = project;
        return this;
    }

    public SLURMSSHJobBuilder wallTime(Long wallTime) {
        this.wallTime = wallTime;
        return this;
    }

    public SLURMSSHJobBuilder hostCount(Integer hostCount) {
        this.hostCount = hostCount;
        return this;
    }

    public SLURMSSHJob build() {
        return new SLURMSSHJob(this);
    }

}
