package org.renci.jlrm.condor.cli;

import java.io.File;
import java.util.concurrent.Callable;

import org.renci.jlrm.JLRMException;
import org.renci.jlrm.condor.CondorDAGLogParser;
import org.renci.jlrm.condor.CondorJobStatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CondorLookupDAGStatusCallable implements Callable<CondorJobStatusType> {

    private final Logger logger = LoggerFactory.getLogger(CondorLookupDAGStatusCallable.class);

    private File dagmanOutFile;

    public CondorLookupDAGStatusCallable() {
        super();
    }

    public CondorLookupDAGStatusCallable(File dagmanOutFile) {
        super();
        this.dagmanOutFile = dagmanOutFile;
    }

    @Override
    public CondorJobStatusType call() throws JLRMException {
        logger.debug("ENTERING call()");
        CondorJobStatusType ret = CondorDAGLogParser.getInstance().parse(dagmanOutFile);
        return ret;
    }

    public File getDagmanOutFile() {
        return dagmanOutFile;
    }

    public void setDagmanOutFile(File dagmanOutFile) {
        this.dagmanOutFile = dagmanOutFile;
    }

}
