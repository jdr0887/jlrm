package org.renci.jlrm.sge.cli;

import java.io.File;

import org.renci.jlrm.JLRMException;
import org.renci.jlrm.sge.SGEJob;
import org.renci.jlrm.sge.SGEJobStatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author jdr0887
 */
public class SGECLIFactory {

    private final Logger logger = LoggerFactory.getLogger(SGECLIFactory.class);

    private static SGECLIFactory instance = null;

    public static SGECLIFactory getInstance() {
        if (instance == null) {
            instance = new SGECLIFactory();
        }
        return instance;
    }

    private SGECLIFactory() {
        super();
    }

    public SGEJob submit(File submitDir, SGEJob job) throws JLRMException {
        logger.debug("ENTERING submit(File)");
        SGESubmitCallable runnable = new SGESubmitCallable(job, submitDir);
        return runnable.call();
    }

    public SGEJobStatusType lookupStatus(SGEJob job) throws JLRMException {
        logger.debug("ENTERING lookupStatus(job)");
        SGELookupStatusCallable runnable = new SGELookupStatusCallable(job);
        return runnable.call();
    }

}
