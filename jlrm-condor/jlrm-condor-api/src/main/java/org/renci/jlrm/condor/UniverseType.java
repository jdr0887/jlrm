package org.renci.jlrm.condor;

public enum UniverseType {

    STANDARD(1),

    VANILLA(5),

    SCHEDULER(7),

    MPI(8),

    GRID(9),

    JAVA(10),

    PARALLEL(11),

    LOCAL(12);

    private int code;

    private UniverseType(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

}
