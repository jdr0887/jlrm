package org.renci.jlrm.sge.cli;

import java.io.File;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.StringUtils;
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

    private File sgeHomeDirectory;

    public static SGECLIFactory getInstance() throws JLRMException {
        if (instance == null) {
            instance = new SGECLIFactory();
        }
        return instance;
    }

    private SGECLIFactory() throws JLRMException {
        super();
        String sgeHome = System.getenv("SGE_HOME");
        if (StringUtils.isEmpty(sgeHome)) {
            logger.error("SGE_HOME not set in env: {}", sgeHome);
            throw new JLRMException("SGE_HOME not set in env");
        }
        this.sgeHomeDirectory = new File(sgeHome);
        if (!sgeHomeDirectory.exists()) {
            logger.error("SGE_HOME does not exist: {}", sgeHomeDirectory);
            throw new JLRMException("SGE_HOME does not exist");
        }
    }

    public SGEJob submit(File submitDir, SGEJob job) throws JLRMException {
        logger.debug("ENTERING submit(File)");
        SGESubmitCallable runnable = new SGESubmitCallable(this.sgeHomeDirectory, job, submitDir);
        return runnable.call();
    }

    public SGEJobStatusType lookupStatus(SGEJob job) throws JLRMException {
        logger.debug("ENTERING lookupStatus(job)");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        SGEJobStatusType ret = null;
        try {
            ret = executor.submit(new SGELookupStatusCallable(this.sgeHomeDirectory, job)).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return ret;
    }

}
