package org.renci.jlrm;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "Queue", propOrder = {})
@XmlRootElement(name = "queue")
public class Queue {

    private String name;

    private Double weight;

    private Long runTime;

    private Integer maxPending;

    private Integer maxRunning;

    private Integer numberOfProcessors;

    public Queue() {
        super();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getWeight() {
        return weight;
    }

    public void setWeight(Double weight) {
        this.weight = weight;
    }

    public Long getRunTime() {
        return runTime;
    }

    public void setRunTime(Long runTime) {
        this.runTime = runTime;
    }

    public Integer getMaxPending() {
        return maxPending;
    }

    public void setMaxPending(Integer maxPending) {
        this.maxPending = maxPending;
    }

    public Integer getMaxRunning() {
        return maxRunning;
    }

    public void setMaxRunning(Integer maxRunning) {
        this.maxRunning = maxRunning;
    }

    public Integer getNumberOfProcessors() {
        return numberOfProcessors;
    }

    public void setNumberOfProcessors(Integer numberOfProcessors) {
        this.numberOfProcessors = numberOfProcessors;
    }

    @Override
    public String toString() {
        return String.format(
                "Queue [name=%s, weight=%s, runTime=%s, maxPending=%s, maxRunning=%s, numberOfProcessors=%s]", name,
                weight, runTime, maxPending, maxRunning, numberOfProcessors);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((maxPending == null) ? 0 : maxPending.hashCode());
        result = prime * result + ((maxRunning == null) ? 0 : maxRunning.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((numberOfProcessors == null) ? 0 : numberOfProcessors.hashCode());
        result = prime * result + ((runTime == null) ? 0 : runTime.hashCode());
        result = prime * result + ((weight == null) ? 0 : weight.hashCode());
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
        Queue other = (Queue) obj;
        if (maxPending == null) {
            if (other.maxPending != null)
                return false;
        } else if (!maxPending.equals(other.maxPending))
            return false;
        if (maxRunning == null) {
            if (other.maxRunning != null)
                return false;
        } else if (!maxRunning.equals(other.maxRunning))
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
        if (runTime == null) {
            if (other.runTime != null)
                return false;
        } else if (!runTime.equals(other.runTime))
            return false;
        if (weight == null) {
            if (other.weight != null)
                return false;
        } else if (!weight.equals(other.weight))
            return false;
        return true;
    }

}
