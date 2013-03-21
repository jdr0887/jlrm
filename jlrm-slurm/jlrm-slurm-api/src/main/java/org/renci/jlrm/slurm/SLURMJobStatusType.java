package org.renci.jlrm.slurm;

public enum SLURMJobStatusType {

    DELETION("d"),

    ERROR("E"),

    HOLD("h"),

    RUNNING("r"),

    RESTARTED("R"),

    SUSPENDED("s"),

    TRANSFERING("t"),

    THRESHOLD("T"),

    WAITING("qw"),

    DONE;

    private String value;

    private SLURMJobStatusType(String value) {
        this.value = value;
    }

    private SLURMJobStatusType() {
        this.value = "";
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

}
