package org.renci.jlrm.lsf;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.renci.jlrm.JobBuilder;

public class LSFJobBuilder extends JobBuilder {

    protected String queueName;

    protected String project;

    protected Long wallTime;

    protected Integer hostCount;

    public LSFJobBuilder() {
        super();
    }

    public LSFJobBuilder id(String id) {
        this.id = id;
        return this;
    }

    public LSFJobBuilder name(String name) {
        this.name = name;
        return this;
    }

    public LSFJobBuilder executable(File executable) {
        this.executable = executable;
        return this;
    }

    public LSFJobBuilder submitFile(File submitFile) {
        this.submitFile = submitFile;
        return this;
    }

    public LSFJobBuilder output(File output) {
        this.output = output;
        return this;
    }

    public LSFJobBuilder error(File error) {
        this.error = error;
        return this;
    }

    public LSFJobBuilder numberOfProcessors(Integer numberOfProcessors) {
        this.numberOfProcessors = numberOfProcessors;
        return this;
    }

    public LSFJobBuilder memory(String memory) {
        this.memory = memory;
        return this;
    }

    public LSFJobBuilder duration(long duration) {
        this.duration = duration;
        return this;
    }

    public LSFJobBuilder durationTimeUnit(TimeUnit durationTimeUnit) {
        this.durationTimeUnit = durationTimeUnit;
        return this;
    }

    public String queueName() {
        return queueName;
    }

    public LSFJobBuilder queueName(String queueName) {
        this.queueName = queueName;
        return this;
    }

    public String project() {
        return project;
    }

    public LSFJobBuilder project(String project) {
        this.project = project;
        return this;
    }

    public Long wallTime() {
        return wallTime;
    }

    public LSFJobBuilder wallTime(Long wallTime) {
        this.wallTime = wallTime;
        return this;
    }

    public Integer hostCount() {
        return hostCount;
    }

    public LSFJobBuilder hostCount(Integer hostCount) {
        this.hostCount = hostCount;
        return this;
    }

    public LSFJob build() {
        return new LSFJob(this);
    }

}
