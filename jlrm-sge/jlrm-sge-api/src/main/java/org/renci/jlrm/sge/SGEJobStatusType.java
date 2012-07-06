package org.renci.jlrm.sge;

public enum SGEJobStatusType {

    DELETION("d"),

    ERROR("E"),

    HOLD("h"),

    RUNNING("r"),

    RESTARTED("R"),

    SUSPENDED("s"),

    TRANSFERING("t"),

    THRESHOLD("T"),

    WAITING("w"),
    
    DONE;

    private String value;

    private SGEJobStatusType(String value) {
        this.value = value;
    }

    private SGEJobStatusType() {
        this.value = "";
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

}
