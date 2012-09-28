package org.renci.jlrm.lsf.cli;

import java.io.File;

import org.renci.jlrm.JLRMException;
import org.renci.jlrm.lsf.LSFJob;
import org.renci.jlrm.lsf.LSFJobStatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author jdr0887
 */
public class LSFCLIFactory {

    private final Logger logger = LoggerFactory.getLogger(LSFCLIFactory.class);

    private static LSFCLIFactory instance = null;

    public static LSFCLIFactory getInstance() {
        if (instance == null) {
            instance = new LSFCLIFactory();
        }
        return instance;
    }

    private LSFCLIFactory() {
        super();
    }

    public LSFJob submit(File submitDir, LSFJob job) throws JLRMException {
        logger.debug("ENTERING submit(File)");
        LSFSubmitCallable runnable = new LSFSubmitCallable(job, submitDir);
        return runnable.call();
    }

    public LSFJobStatusType lookupStatus(LSFJob job) throws JLRMException {
        logger.debug("ENTERING lookupStatus(job)");
        LSFLookupStatusCallable runnable = new LSFLookupStatusCallable(job);
        return runnable.call();
    }

}
