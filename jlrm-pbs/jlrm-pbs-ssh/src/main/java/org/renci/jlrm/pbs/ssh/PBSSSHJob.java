package org.renci.jlrm.pbs.ssh;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.renci.jlrm.pbs.PBSJob;

public class PBSSSHJob extends PBSJob {

    private static final long serialVersionUID = 5783144158593562541L;

    private Boolean transferInputs = Boolean.FALSE;

    private Boolean transferExecutable = Boolean.FALSE;

    private List<File> inputFiles = new ArrayList<File>();

    public PBSSSHJob() {
        super();
    }

    public PBSSSHJob(PBSSSHJobBuilder builder) {
        super();
        // from JobBuilder
        this.id = builder.id();
        this.name = builder.name();
        this.executable = builder.executable();
        this.submitFile = builder.submitFile();
        this.output = builder.output();
        this.error = builder.error();
        this.numberOfProcessors = builder.numberOfProcessors();
        this.memory = builder.memory();
        this.disk = builder.disk();
        this.duration = builder.duration();
        this.durationTimeUnit = builder.durationTimeUnit();
        // from LSFJobBuilder
        this.queueName = builder.queueName();
        this.project = builder.project();
        this.wallTime = builder.wallTime();
        this.hostCount = builder.hostCount();
        // from LSFJobBuilder
        this.transferInputs = builder.transferInputs();
        this.transferExecutable = builder.transferExecutable();
        this.inputFiles = builder.inputFiles();
    }

    public Boolean getTransferInputs() {
        return transferInputs;
    }

    public void setTransferInputs(Boolean transferInputs) {
        this.transferInputs = transferInputs;
    }

    public Boolean getTransferExecutable() {
        return transferExecutable;
    }

    public void setTransferExecutable(Boolean transferExecutable) {
        this.transferExecutable = transferExecutable;
    }

    public List<File> getInputFiles() {
        return inputFiles;
    }

    public void setInputFiles(List<File> inputFiles) {
        this.inputFiles = inputFiles;
    }

}
