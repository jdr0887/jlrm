package org.renci.jlrm.pbs.cli;

import java.io.File;

import org.renci.jlrm.JLRMException;
import org.renci.jlrm.pbs.PBSJob;
import org.renci.jlrm.pbs.PBSJobStatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author jdr0887
 */
public class PBSCLIFactory {

    private final Logger logger = LoggerFactory.getLogger(PBSCLIFactory.class);

    private static PBSCLIFactory instance = null;

    public static PBSCLIFactory getInstance() {
        if (instance == null) {
            instance = new PBSCLIFactory();
        }
        return instance;
    }

    private PBSCLIFactory() {
        super();
    }

    public PBSJob submit(File submitDir, PBSJob job) throws JLRMException {
        logger.debug("ENTERING submit(File)");
        PBSSubmitCallable runnable = new PBSSubmitCallable(job, submitDir);
        return runnable.call();
    }

    public PBSJobStatusType lookupStatus(PBSJob job) throws JLRMException {
        logger.debug("ENTERING lookupStatus(job)");
        PBSLookupStatusCallable runnable = new PBSLookupStatusCallable(job);
        return runnable.call();
    }

}
