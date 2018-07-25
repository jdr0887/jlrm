package org.renci.jlrm.slurm.ssh;

import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.JobStatusInfo;
import org.renci.jlrm.Site;
import org.renci.jlrm.commons.ssh.SSHConnectionUtil;
import org.renci.jlrm.slurm.SLURMJobStatusType;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Slf4j
public class SLURMSSHLookupStatusCallable implements Callable<Set<JobStatusInfo>> {

    private Site site;

    private String id;

    @Override
    public Set<JobStatusInfo> call() throws JLRMException {

        Set<JobStatusInfo> jobStatusSet = new HashSet<JobStatusInfo>();
        try {

            Calendar calendar = Calendar.getInstance();
            calendar.setTime(new Date());
            calendar.roll(Calendar.DAY_OF_YEAR, -4);
            String dateFormat = DateFormatUtils.format(calendar, "MMdd");

            String command = String.format(
                    "sacct -S %s -P -o JobID -o State -o Partition -o JobName | grep -v \"\\.batch\"", dateFormat);

            if (StringUtils.isNotEmpty(id)) {
                command = String.format(
                        "sacct -S %s -P -o JobID -o State -o Partition -o JobName | grep %s | grep -v \"\\.batch\"",
                        dateFormat, id);
            }

            String output = SSHConnectionUtil.execute(command, site.getUsername(), getSite().getSubmitHost());

            try (StringReader sr = new StringReader(output); LineNumberReader lnr = new LineNumberReader(sr)) {
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
                            log.debug("JobStatus is {}", info.toString());
                            jobStatusSet.add(info);
                        }
                    }
                }

            }

        } catch (Exception e) {
            log.error("Exception", e);
            throw new JLRMException("Exception: " + e.getMessage());
        }
        return jobStatusSet;
    }

}
