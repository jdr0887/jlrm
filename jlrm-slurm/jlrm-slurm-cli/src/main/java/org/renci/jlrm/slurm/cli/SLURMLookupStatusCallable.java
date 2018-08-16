package org.renci.jlrm.slurm.cli;

import java.io.File;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.renci.common.exec.BashExecutor;
import org.renci.common.exec.CommandInput;
import org.renci.common.exec.CommandOutput;
import org.renci.common.exec.Executor;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.JobStatusInfo;
import org.renci.jlrm.slurm.SLURMJob;
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
public class SLURMLookupStatusCallable implements Callable<Set<JobStatusInfo>> {

    private SLURMJob job;

    @Override
    public Set<JobStatusInfo> call() throws JLRMException {

        Set<JobStatusInfo> jobStatusSet = new HashSet<JobStatusInfo>();

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.roll(Calendar.DAY_OF_YEAR, -4);
        String dateFormat = DateFormatUtils.format(calendar, "MMdd");

        String command = String
                .format("sacct -S %s -P -o JobID -o State -o Partition -o JobName | grep -v \"\\.batch\"", dateFormat);

        if (StringUtils.isNotEmpty(job.getId())) {
            command = String.format(
                    "sacct -S %s -P -o JobID -o State -o Partition -o JobName | grep %s | grep -v \"\\.batch\"",
                    dateFormat, job.getId());
        }

        try {

            CommandInput input = new CommandInput(command, job.getSubmitFile().getParentFile());
            input.setExitImmediately(Boolean.FALSE);
            Executor executor = BashExecutor.getInstance();
            CommandOutput output = executor.execute(input, new File(System.getProperty("user.home"), ".bashrc"));
            String stdout = output.getStdout().toString();

            LineNumberReader lnr = new LineNumberReader(new StringReader(stdout));
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

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new JLRMException("Problem running: " + command);
        }
        return jobStatusSet;

    }

}
