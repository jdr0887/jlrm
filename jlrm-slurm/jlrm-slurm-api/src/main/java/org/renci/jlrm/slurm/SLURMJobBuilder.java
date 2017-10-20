package org.renci.jlrm.slurm;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.Range;
import org.renci.jlrm.JobBuilder;

public class SLURMJobBuilder extends JobBuilder {

    protected String queueName;

    protected String project;

    protected Long wallTime;

    protected Integer hostCount;

    protected String constraint;

    protected Range<Integer> array;

    public SLURMJobBuilder() {
        super();
    }

    public SLURMJobBuilder array(Range<Integer> array) {
        this.array = array;
        return this;
    }

    public Range<Integer> array() {
        return this.array;
    }

    public SLURMJobBuilder id(String id) {
        this.id = id;
        return this;
    }

    public SLURMJobBuilder name(String name) {
        this.name = name;
        return this;
    }

    public SLURMJobBuilder executable(File executable) {
        this.executable = executable;
        return this;
    }

    public SLURMJobBuilder submitFile(File submitFile) {
        this.submitFile = submitFile;
        return this;
    }

    public SLURMJobBuilder output(File output) {
        this.output = output;
        return this;
    }

    public SLURMJobBuilder error(File error) {
        this.error = error;
        return this;
    }

    public SLURMJobBuilder numberOfProcessors(Integer numberOfProcessors) {
        this.numberOfProcessors = numberOfProcessors;
        return this;
    }

    public SLURMJobBuilder memory(String memory) {
        this.memory = memory;
        return this;
    }

    public SLURMJobBuilder duration(long duration) {
        this.duration = duration;
        return this;
    }

    public SLURMJobBuilder durationTimeUnit(TimeUnit durationTimeUnit) {
        this.durationTimeUnit = durationTimeUnit;
        return this;
    }

    public String queueName() {
        return queueName;
    }

    public SLURMJobBuilder queueName(String queueName) {
        this.queueName = queueName;
        return this;
    }

    public String project() {
        return project;
    }

    public SLURMJobBuilder project(String project) {
        this.project = project;
        return this;
    }

    public String constraint() {
        return constraint;
    }

    public SLURMJobBuilder constraint(String constraint) {
        this.constraint = constraint;
        return this;
    }

    public Long wallTime() {
        return wallTime;
    }

    public SLURMJobBuilder wallTime(Long wallTime) {
        this.wallTime = wallTime;
        return this;
    }

    public Integer hostCount() {
        return hostCount;
    }

    public SLURMJobBuilder hostCount(Integer hostCount) {
        this.hostCount = hostCount;
        return this;
    }

    public SLURMJob build() {
        return new SLURMJob(this);
    }

}
