package org.renci.jlrm.slurm.ssh;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.Site;
import org.renci.jlrm.commons.ssh.SSHConnectionUtil;
import org.renci.jlrm.slurm.SLURMJobStatusInfo;
import org.renci.jlrm.slurm.SLURMJobStatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SLURMSSHLookupStatusCallable implements Callable<Set<SLURMJobStatusInfo>> {

    private final Logger logger = LoggerFactory.getLogger(SLURMSSHLookupStatusCallable.class);

    private Site site;

    private List<SLURMSSHJob> jobs;

    public SLURMSSHLookupStatusCallable() {
        super();
    }

    public SLURMSSHLookupStatusCallable(Site site, List<SLURMSSHJob> jobs) {
        super();
        this.site = site;
        this.jobs = jobs;
    }

    @Override
    public Set<SLURMJobStatusInfo> call() throws JLRMException {
        logger.info("ENTERING call()");

        Set<SLURMJobStatusInfo> jobStatusSet = new HashSet<SLURMJobStatusInfo>();
        try {

            Calendar calendar = Calendar.getInstance();
            calendar.setTime(new Date());
            calendar.roll(Calendar.DAY_OF_YEAR, -4);
            String dateFormat = DateFormatUtils.format(calendar, "MMdd");

            List<String> jobIdList = new ArrayList<String>();
            if (this.jobs != null && this.jobs.size() > 0) {
                for (SLURMSSHJob job : this.jobs) {
                    jobIdList.add(job.getId());
                }
            }

            String format = "(%s && %s) | sort | uniq";

            String delimitedJobList = jobIdList != null && jobIdList.size() > 0 ? String.format("-j %s",
                    StringUtils.join(jobIdList, ",")) : "";
            String command = String.format(format,
                    String.format(
                            "sacct -S %s -P -o JobID -o State -o Partition -o JobName | grep -v batch | tail -n+2",
                            dateFormat), String.format(
                            "sacct -S %s -P %s -o JobID -o State -o Partition -o JobName | grep -v batch | tail -n+2",
                            dateFormat, delimitedJobList));
            String output = SSHConnectionUtil.execute(command, site.getUsername(), getSite().getSubmitHost());

            LineNumberReader lnr = new LineNumberReader(new StringReader(output));
            String line;
            while ((line = lnr.readLine()) != null) {
                SLURMJobStatusType statusType = SLURMJobStatusType.COMPLETED;
                if (StringUtils.isNotEmpty(line)) {
                    String[] lineSplit = StringUtils.split(line, '|');
                    if (lineSplit != null && lineSplit.length == 4) {
                        for (SLURMJobStatusType type : SLURMJobStatusType.values()) {
                            if (type.toString().equals(lineSplit[1])) {
                                statusType = type;
                                break;
                            }
                        }
                        SLURMJobStatusInfo info = new SLURMJobStatusInfo(lineSplit[0], statusType, lineSplit[2],
                                lineSplit[3]);
                        logger.info("JobStatus is {}", info.toString());
                        jobStatusSet.add(info);
                    }
                }
            }

        } catch (IOException e) {
            logger.error("IOException", e);
            throw new JLRMException("IOException: " + e.getMessage());
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

    public List<SLURMSSHJob> getJobs() {
        return jobs;
    }

    public void setJobs(List<SLURMSSHJob> jobs) {
        this.jobs = jobs;
    }

}
