package org.renci.jlrm.slurm.ssh;

import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.JobStatusInfo;
import org.renci.jlrm.Site;
import org.renci.jlrm.commons.ssh.SSHConnectionUtil;
import org.renci.jlrm.slurm.SLURMJobStatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SLURMSSHLookupStatusCallable implements Callable<Set<JobStatusInfo>> {

    private static final Logger logger = LoggerFactory.getLogger(SLURMSSHLookupStatusCallable.class);

    private Site site;

    public SLURMSSHLookupStatusCallable() {
        super();
    }

    public SLURMSSHLookupStatusCallable(Site site) {
        super();
        this.site = site;
    }

    @Override
    public Set<JobStatusInfo> call() throws JLRMException {
        logger.debug("ENTERING call()");

        Set<JobStatusInfo> jobStatusSet = new HashSet<JobStatusInfo>();
        try {

            Calendar calendar = Calendar.getInstance();
            calendar.setTime(new Date());
            calendar.roll(Calendar.DAY_OF_YEAR, -4);
            String dateFormat = DateFormatUtils.format(calendar, "MMdd");

            String command = String.format(
                    "sacct -S %s -P -o JobID -o State -o Partition -o JobName | grep -v \"\\.batch\" | tail -n+2",
                    dateFormat);

            String output = SSHConnectionUtil.execute(command, site.getUsername(), getSite().getSubmitHost());

            LineNumberReader lnr = new LineNumberReader(new StringReader(output));
            String line;
            while ((line = lnr.readLine()) != null) {
                SLURMJobStatusType statusType = SLURMJobStatusType.COMPLETED;
                if (StringUtils.isNotEmpty(line)) {
                    String[] lineSplit = StringUtils.split(line, '|');
                    if (lineSplit != null && lineSplit.length == 4) {
                        for (SLURMJobStatusType type : SLURMJobStatusType.values()) {
                            if (StringUtils.isNotEmpty(lineSplit[1]) && lineSplit[1].contains(type.toString())) {
                                statusType = type;
                                break;
                            }
                        }
                        JobStatusInfo info = new JobStatusInfo(lineSplit[0], statusType.toString(), lineSplit[2],
                                lineSplit[3]);
                        logger.debug("JobStatus is {}", info.toString());
                        jobStatusSet.add(info);
                    }
                }
            }

        } catch (Exception e) {
            logger.error("Exception", e);
            throw new JLRMException("Exception: " + e.getMessage());
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
