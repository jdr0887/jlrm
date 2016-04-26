package org.renci.jlrm.sge;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.renci.jlrm.JobBuilder;

public class SGEJobBuilder extends JobBuilder {

    protected String queueName;

    protected String project;

    protected Long wallTime;

    protected Integer hostCount;

    public SGEJobBuilder() {
        super();
    }

    public SGEJobBuilder id(String id) {
        this.id = id;
        return this;
    }

    public SGEJobBuilder name(String name) {
        this.name = name;
        return this;
    }

    public SGEJobBuilder executable(File executable) {
        this.executable = executable;
        return this;
    }

    public SGEJobBuilder submitFile(File submitFile) {
        this.submitFile = submitFile;
        return this;
    }

    public SGEJobBuilder output(File output) {
        this.output = output;
        return this;
    }

    public SGEJobBuilder error(File error) {
        this.error = error;
        return this;
    }

    public SGEJobBuilder numberOfProcessors(Integer numberOfProcessors) {
        this.numberOfProcessors = numberOfProcessors;
        return this;
    }

    public SGEJobBuilder memory(String memory) {
        this.memory = memory;
        return this;
    }

    public SGEJobBuilder duration(long duration) {
        this.duration = duration;
        return this;
    }

    public SGEJobBuilder durationTimeUnit(TimeUnit durationTimeUnit) {
        this.durationTimeUnit = durationTimeUnit;
        return this;
    }

    public String queueName() {
        return queueName;
    }

    public SGEJobBuilder queueName(String queueName) {
        this.queueName = queueName;
        return this;
    }

    public String project() {
        return project;
    }

    public SGEJobBuilder project(String project) {
        this.project = project;
        return this;
    }

    public Long wallTime() {
        return wallTime;
    }

    public SGEJobBuilder wallTime(Long wallTime) {
        this.wallTime = wallTime;
        return this;
    }

    public Integer hostCount() {
        return hostCount;
    }

    public SGEJobBuilder hostCount(Integer hostCount) {
        this.hostCount = hostCount;
        return this;
    }

    public SGEJob build() {
        return new SGEJob(this);
    }

}
