package org.renci.jlrm.slurm.ssh;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.renci.jlrm.slurm.SLURMJob;

public class SLURMSSHJob extends SLURMJob {

    private Boolean transferInputs = Boolean.FALSE;

    private Boolean transferExecutable = Boolean.FALSE;

    private List<File> inputFiles = new ArrayList<File>();

    public SLURMSSHJob() {
        super();
    }

    public SLURMSSHJob(String name, File executable) {
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
