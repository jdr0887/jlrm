package org.renci.jlrm;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class JobBuilder {

    protected String id;

    protected String name;

    protected File executable;

    protected File submitFile;

    protected File output;

    protected File error;

    protected Integer numberOfProcessors = 1;

    protected String memory = "2GB";

    protected String disk = "5GB";

    protected long duration;

    protected TimeUnit durationTimeUnit;

    public JobBuilder() {
        super();
    }

    public String id() {
        return id;
    }

    public JobBuilder id(String id) {
        this.id = id;
        return this;
    }

    public String disk() {
        return disk;
    }

    public JobBuilder disk(String disk) {
        this.disk = disk;
        return this;
    }

    public String name() {
        return name;
    }

    public JobBuilder name(String name) {
        this.name = name;
        return this;
    }

    public File executable() {
        return executable;
    }

    public JobBuilder executable(File executable) {
        this.executable = executable;
        return this;
    }

    public File submitFile() {
        return submitFile;
    }

    public JobBuilder submitFile(File submitFile) {
        this.submitFile = submitFile;
        return this;
    }

    public File output() {
        return output;
    }

    public JobBuilder output(File output) {
        this.output = output;
        return this;
    }

    public File error() {
        return error;
    }

    public JobBuilder error(File error) {
        this.error = error;
        return this;
    }

    public Integer numberOfProcessors() {
        return numberOfProcessors;
    }

    public JobBuilder numberOfProcessors(Integer numberOfProcessors) {
        this.numberOfProcessors = numberOfProcessors;
        return this;
    }

    public String memory() {
        return memory;
    }

    public JobBuilder memory(String memory) {
        this.memory = memory;
        return this;
    }

    public long duration() {
        return duration;
    }

    public JobBuilder duration(long duration) {
        this.duration = duration;
        return this;
    }

    public TimeUnit durationTimeUnit() {
        return durationTimeUnit;
    }

    public JobBuilder durationTimeUnit(TimeUnit durationTimeUnit) {
        this.durationTimeUnit = durationTimeUnit;
        return this;
    }

    public Job build() {
        return new Job(this);
    }
}
