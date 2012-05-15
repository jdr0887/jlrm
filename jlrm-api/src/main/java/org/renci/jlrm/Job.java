package org.renci.jlrm;

import java.io.File;

public class Job {

    protected String id;

    protected String name;

    protected File executable;

    protected File submitFile;

    protected File output;

    protected File error;

    protected Integer numberOfProcessors = 1;

    protected Integer memory = 4 * 1024;

    public Job() {
        super();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public File getExecutable() {
        return executable;
    }

    public void setExecutable(File executable) {
        this.executable = executable;
    }

    public File getSubmitFile() {
        return submitFile;
    }

    public void setSubmitFile(File submitFile) {
        this.submitFile = submitFile;
    }

    public File getOutput() {
        return output;
    }

    public void setOutput(File output) {
        this.output = output;
    }

    public File getError() {
        return error;
    }

    public void setError(File error) {
        this.error = error;
    }

    public Integer getNumberOfProcessors() {
        return numberOfProcessors;
    }

    public void setNumberOfProcessors(Integer numberOfProcessors) {
        this.numberOfProcessors = numberOfProcessors;
    }

    public Integer getMemory() {
        return memory;
    }

    public void setMemory(Integer memory) {
        this.memory = memory;
    }

    @Override
    public String toString() {
        return "Job [id=" + id + ", name=" + name + ", executable=" + executable + ", submitFile=" + submitFile
                + ", output=" + output + ", error=" + error + ", numberOfProcessors=" + numberOfProcessors
                + ", memory=" + memory + "]";
    }

}
