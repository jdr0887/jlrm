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

    private File lsfHome;

    public static LSFCLIFactory getInstance(File lsfHome) {
        if (instance == null) {
            instance = new LSFCLIFactory(lsfHome);
        }
        return instance;
    }

    private LSFCLIFactory(File lsfHome) {
        super();
        this.lsfHome = lsfHome;
    }

    public LSFJob submit(File submitDir, LSFJob job) throws JLRMException {
        logger.debug("ENTERING submit(File)");
        LSFSubmitCallable runnable = new LSFSubmitCallable(this.lsfHome, job, submitDir);
        return runnable.call();
    }

    public LSFJobStatusType lookupStatus(LSFJob job) throws JLRMException {
        logger.debug("ENTERING lookupStatus(job)");
        LSFLookupStatusCallable runnable = new LSFLookupStatusCallable(this.lsfHome, job);
        return runnable.call();
    }

}
