package org.renci.jlrm.pbs.ssh;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.renci.jlrm.pbs.PBSJob;

public class PBSSSHJob extends PBSJob {

    private Boolean transferInputs = Boolean.FALSE;

    private Boolean transferExecutable = Boolean.FALSE;

    private List<File> inputFiles = new ArrayList<File>();

    public PBSSSHJob() {
        super();
    }

    public PBSSSHJob(String name, File executable) {
        super(name, executable);
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
