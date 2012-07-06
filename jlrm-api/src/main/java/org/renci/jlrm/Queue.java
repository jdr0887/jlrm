package org.renci.jlrm;


public class Queue {

    private String name;

    private long pendingTime;

    private Double weight;

    private long runTime;

    private int maxJobLimit;

    private int maxMultipleJobsToSubmit;

    public Queue() {
        super();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getPendingTime() {
        return pendingTime;
    }

    public void setPendingTime(long pendingTime) {
        this.pendingTime = pendingTime;
    }

    public Double getWeight() {
        return weight;
    }

    public void setWeight(Double weight) {
        this.weight = weight;
    }

    public long getRunTime() {
        return runTime;
    }

    public void setRunTime(long runTime) {
        this.runTime = runTime;
    }

    public int getMaxJobLimit() {
        return maxJobLimit;
    }

    public void setMaxJobLimit(int maxJobLimit) {
        this.maxJobLimit = maxJobLimit;
    }

    public int getMaxMultipleJobsToSubmit() {
        return maxMultipleJobsToSubmit;
    }

    public void setMaxMultipleJobsToSubmit(int maxMultipleJobsToSubmit) {
        this.maxMultipleJobsToSubmit = maxMultipleJobsToSubmit;
    }

    @Override
    public String toString() {
        return "Queue [name=" + name + ", pendingTime=" + pendingTime + ", weight=" + weight + ", runTime=" + runTime
                + ", maxJobLimit=" + maxJobLimit + ", maxMultipleJobsToSubmit=" + maxMultipleJobsToSubmit + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + maxJobLimit;
        result = prime * result + maxMultipleJobsToSubmit;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + (int) (pendingTime ^ (pendingTime >>> 32));
        result = prime * result + (int) (runTime ^ (runTime >>> 32));
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
        if (maxJobLimit != other.maxJobLimit)
            return false;
        if (maxMultipleJobsToSubmit != other.maxMultipleJobsToSubmit)
            return false;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (pendingTime != other.pendingTime)
            return false;
        if (runTime != other.runTime)
            return false;
        if (weight == null) {
            if (other.weight != null)
                return false;
        } else if (!weight.equals(other.weight))
            return false;
        return true;
    }

}
