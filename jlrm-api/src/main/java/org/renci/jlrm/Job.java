package org.renci.jlrm;

import java.io.File;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "Job", propOrder = {})
@XmlRootElement(name = "job")
public class Job implements Serializable {

    private static final long serialVersionUID = 5760998445998689534L;

    @XmlAttribute
    protected String id;

    @XmlAttribute
    protected String name;

    protected File executable;

    protected File submitFile;

    protected File output;

    protected File error;

    protected Integer numberOfProcessors = 1;

    protected String memory = "2048";

    protected String disk;

    protected long duration;

    protected TimeUnit durationTimeUnit;

    public Job() {
        super();
    }

    public Job(JobBuilder jobBuilder) {
        super();
        this.id = jobBuilder.id();
        this.name = jobBuilder.name();
        this.executable = jobBuilder.executable();
        this.submitFile = jobBuilder.submitFile();
        this.output = jobBuilder.output();
        this.error = jobBuilder.error();
        this.numberOfProcessors = jobBuilder.numberOfProcessors();
        this.memory = jobBuilder.memory();
        this.duration = jobBuilder.duration();
        this.durationTimeUnit = jobBuilder.durationTimeUnit();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDisk() {
        return disk;
    }

    public void setDisk(String disk) {
        this.disk = disk;
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

    public String getMemory() {
        return memory;
    }

    public void setMemory(String memory) {
        this.memory = memory;
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    public TimeUnit getDurationTimeUnit() {
        return durationTimeUnit;
    }

    public void setDurationTimeUnit(TimeUnit durationTimeUnit) {
        this.durationTimeUnit = durationTimeUnit;
    }

    @Override
    public String toString() {
        return String.format(
                "Job [id=%s, name=%s, executable=%s, submitFile=%s, output=%s, error=%s, numberOfProcessors=%s, memory=%s, disk=%s, duration=%s, durationTimeUnit=%s]",
                id, name, executable, submitFile, output, error, numberOfProcessors, memory, disk, duration,
                durationTimeUnit);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (duration ^ (duration >>> 32));
        result = prime * result + ((durationTimeUnit == null) ? 0 : durationTimeUnit.hashCode());
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
        if (duration != other.duration)
            return false;
        if (durationTimeUnit != other.durationTimeUnit)
            return false;
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
