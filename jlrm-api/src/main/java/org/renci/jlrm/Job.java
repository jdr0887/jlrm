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

    public Job(String name, File executable) {
        super();
        this.name = name;
        this.executable = executable;
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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((error == null) ? 0 : error.hashCode());
        result = prime * result + ((executable == null) ? 0 : executable.hashCode());
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((memory == null) ? 0 : memory.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((numberOfProcessors == null) ? 0 : numberOfProcessors.hashCode());
        result = prime * result + ((output == null) ? 0 : output.hashCode());
        result = prime * result + ((submitFile == null) ? 0 : submitFile.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Job other = (Job) obj;
        if (error == null) {
            if (other.error != null)
                return false;
        } else if (!error.equals(other.error))
            return false;
        if (executable == null) {
            if (other.executable != null)
                return false;
        } else if (!executable.equals(other.executable))
            return false;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        if (memory == null) {
            if (other.memory != null)
                return false;
        } else if (!memory.equals(other.memory))
            return false;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (numberOfProcessors == null) {
            if (other.numberOfProcessors != null)
                return false;
        } else if (!numberOfProcessors.equals(other.numberOfProcessors))
            return false;
        if (output == null) {
            if (other.output != null)
                return false;
        } else if (!output.equals(other.output))
            return false;
        if (submitFile == null) {
            if (other.submitFile != null)
                return false;
        } else if (!submitFile.equals(other.submitFile))
            return false;
        return true;
    }

}
