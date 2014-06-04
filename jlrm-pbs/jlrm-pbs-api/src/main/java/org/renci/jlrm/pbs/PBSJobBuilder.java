package org.renci.jlrm.pbs;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.renci.jlrm.JobBuilder;

public class PBSJobBuilder extends JobBuilder {

    protected String queueName;

    protected String project;

    protected Long wallTime;

    protected Integer hostCount;

    public PBSJobBuilder() {
        super();
    }

    public PBSJobBuilder id(String id) {
        this.id = id;
        return this;
    }

    public PBSJobBuilder name(String name) {
        this.name = name;
        return this;
    }

    public PBSJobBuilder executable(File executable) {
        this.executable = executable;
        return this;
    }

    public PBSJobBuilder submitFile(File submitFile) {
        this.submitFile = submitFile;
        return this;
    }

    public PBSJobBuilder output(File output) {
        this.output = output;
        return this;
    }

    public PBSJobBuilder error(File error) {
        this.error = error;
        return this;
    }

    public PBSJobBuilder numberOfProcessors(Integer numberOfProcessors) {
        this.numberOfProcessors = numberOfProcessors;
        return this;
    }

    public PBSJobBuilder memory(Integer memory) {
        this.memory = memory;
        return this;
    }

    public PBSJobBuilder duration(long duration) {
        this.duration = duration;
        return this;
    }

    public PBSJobBuilder durationTimeUnit(TimeUnit durationTimeUnit) {
        this.durationTimeUnit = durationTimeUnit;
        return this;
    }

    public String queueName() {
        return queueName;
    }

    public PBSJobBuilder queueName(String queueName) {
        this.queueName = queueName;
        return this;
    }

    public String project() {
        return project;
    }

    public PBSJobBuilder project(String project) {
        this.project = project;
        return this;
    }

    public Long wallTime() {
        return wallTime;
    }

    public PBSJobBuilder wallTime(Long wallTime) {
        this.wallTime = wallTime;
        return this;
    }

    public Integer hostCount() {
        return hostCount;
    }

    public PBSJobBuilder hostCount(Integer hostCount) {
        this.hostCount = hostCount;
        return this;
    }

    public PBSJob build() {
        return new PBSJob(this);
    }

}
