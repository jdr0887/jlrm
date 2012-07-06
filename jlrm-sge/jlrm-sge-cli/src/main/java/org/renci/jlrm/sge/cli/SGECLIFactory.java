package org.renci.jlrm.sge.cli;

import java.io.File;

import org.renci.jlrm.LRMException;
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

    private File sgeHome;

    public static SGECLIFactory getInstance(File sgeHome) {
        if (instance == null) {
            instance = new SGECLIFactory(sgeHome);
        }
        return instance;
    }

    private SGECLIFactory(File sgeHome) {
        super();
        this.sgeHome = sgeHome;
    }

    public SGEJob submit(File submitDir, SGEJob job) throws LRMException {
        logger.debug("ENTERING submit(File)");
        SGESubmitCallable runnable = new SGESubmitCallable(this.sgeHome, job, submitDir);
        return runnable.call();
    }

    public SGEJobStatusType lookupStatus(SGEJob job) throws LRMException {
        logger.debug("ENTERING lookupStatus(job)");
        SGELookupStatusCallable runnable = new SGELookupStatusCallable(this.sgeHome, job);
        return runnable.call();
    }

}
