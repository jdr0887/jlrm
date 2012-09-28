package org.renci.jlrm.pbs;

public enum PBSJobStatusType {

    COMPLETE("C"),

    QUEUED("Q"),

    RUNNING("R"),

    ENDING("E"),

    HELD("H"),

    SUSPENDED("S");

    private String value;

    private PBSJobStatusType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

}
