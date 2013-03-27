package org.renci.jlrm.slurm;

public enum SLURMJobStatusType {

    CANCELLED("CA"),

    COMPLETED("CD"),

    CONFIGURING("CF"),

    COMPLETING("CG"),

    FAILED("F"),

    NODE_FAIL("NF"),

    PENDING("PD"),

    PREEMPTED("PR"),

    RUNNING("R"),

    SUSPENDED("S"),

    TIMEOUT("TO");

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
