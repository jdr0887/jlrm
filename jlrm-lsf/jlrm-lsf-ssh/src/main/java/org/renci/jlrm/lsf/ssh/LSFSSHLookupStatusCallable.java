package org.renci.jlrm.lsf.ssh;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.commons.lang.StringUtils;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.Site;
import org.renci.jlrm.commons.ssh.SSHConnectionUtil;
import org.renci.jlrm.lsf.LSFJobStatusInfo;
import org.renci.jlrm.lsf.LSFJobStatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LSFSSHLookupStatusCallable implements Callable<Set<LSFJobStatusInfo>> {

    private final Logger logger = LoggerFactory.getLogger(LSFSSHLookupStatusCallable.class);

    private Site site;

    public LSFSSHLookupStatusCallable() {
        super();
    }

    public LSFSSHLookupStatusCallable(Site site) {
        super();
        this.site = site;
    }

    @Override
    public Set<LSFJobStatusInfo> call() throws JLRMException {
        logger.info("ENTERING call()");

        Set<LSFJobStatusInfo> jobStatusSet = new HashSet<LSFJobStatusInfo>();

        String command = "bjobs -w | tail -n+2 | awk '{print $1,$3,$4,$7}'";
        String output = SSHConnectionUtil.execute(command, site.getUsername(), site.getSubmitHost());

        try {
            LineNumberReader lnr = new LineNumberReader(new StringReader(output));
            String line;
            while ((line = lnr.readLine()) != null) {
                LSFJobStatusType statusType = LSFJobStatusType.DONE;
                if (StringUtils.isNotEmpty(line)) {
                    if (line.contains("is not found")) {
                        statusType = LSFJobStatusType.DONE;
                    } else {
                        String[] lineSplit = line.split(" ");
                        if (lineSplit != null && lineSplit.length == 4) {
                            for (LSFJobStatusType type : LSFJobStatusType.values()) {
                                if (type.getValue().equals(lineSplit[1])) {
                                    statusType = type;
                                }
                            }
                            LSFJobStatusInfo info = new LSFJobStatusInfo(lineSplit[0], statusType, lineSplit[2],
                                    lineSplit[3]);
                            jobStatusSet.add(info);
                        }
                    }
                }
            }
        } catch (IOException e) {
            logger.error("IOException", e);
            throw new JLRMException("IOException: " + e.getMessage());
        }
        return jobStatusSet;
    }

    public Site getSite() {
        return site;
    }

    public void setSite(Site site) {
        this.site = site;
    }

}
