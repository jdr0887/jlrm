package org.renci.jlrm.slurm.cli;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.renci.jlrm.JobStatusInfo;
import org.renci.jlrm.slurm.SLURMJob;
import org.renci.jlrm.slurm.SLURMJobStatusType;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Getter
@Setter
@Slf4j
public class SLURMJobStartMonitor implements Runnable {

    private List<SLURMJobStatusType> failedTypes, finishedTypes, runningTypes, notFinishedTypes;

    private SLURMJob job;

    private Boolean jobFinished;

    private Boolean terminateWhenThereAreFailures;

    public SLURMJobStartMonitor(SLURMJob job) {
        super();
        this.job = job;
        this.terminateWhenThereAreFailures = Boolean.FALSE;
        this.jobFinished = Boolean.FALSE;
        this.failedTypes = new ArrayList<>();
        this.failedTypes.addAll(List.of(SLURMJobStatusType.CANCELLED, SLURMJobStatusType.FAILED,
                SLURMJobStatusType.TIMEOUT, SLURMJobStatusType.NODE_FAIL, SLURMJobStatusType.PREEMPTED));

        this.finishedTypes = new ArrayList<>();
        this.finishedTypes.add(SLURMJobStatusType.COMPLETED);
        this.finishedTypes.addAll(failedTypes);

        this.runningTypes = new ArrayList<>(
                List.of(SLURMJobStatusType.COMPLETING, SLURMJobStatusType.CONFIGURING, SLURMJobStatusType.RUNNING));

        this.notFinishedTypes = new ArrayList<>();
        this.notFinishedTypes.addAll(List.of(SLURMJobStatusType.PENDING, SLURMJobStatusType.SUSPENDED));
        this.notFinishedTypes.addAll(runningTypes);

    }

    @Override
    public void run() {

        try {

            SLURMLookupStatusCallable lookupStatusCallable = new SLURMLookupStatusCallable();
            lookupStatusCallable.setJob(job);

            Set<JobStatusInfo> jobStatusSet = Executors.newSingleThreadExecutor().submit(lookupStatusCallable).get();

            if (job.getArray() == null) {

                JobStatusInfo jobStatusInfo = jobStatusSet.stream().filter(a -> a.getJobId().equals(job.getId()))
                        .findFirst().orElse(null);

                if (jobStatusInfo != null) {
                    log.info(jobStatusInfo.toString());

                    SLURMJobStatusType slurmStatusInfo = Arrays.asList(SLURMJobStatusType.values()).stream()
                            .filter(a -> jobStatusInfo.getStatus().equals(a.toString())).findFirst()
                            .orElse(SLURMJobStatusType.PENDING);

                    if (failedTypes.contains(slurmStatusInfo)) {
                        log.warn("Job failed: {}", jobStatusInfo.toString());
                        jobFinished = true;
                    } else if (finishedTypes.contains(slurmStatusInfo)) {
                        jobFinished = true;
                    } else if (notFinishedTypes.contains(slurmStatusInfo)) {
                        jobFinished = false;
                    } else {
                        jobFinished = true;
                    }

                }

            } else {

                List<JobStatusInfo> relevantJobStatsInfos = jobStatusSet.stream()
                        .filter(a -> a.getJobId().startsWith(job.getId())).collect(Collectors.toList());

                // relevantJobStatsInfos.stream().forEach(a -> log.debug(a.toString()));

                boolean containsFailedJobs = relevantJobStatsInfos.stream()
                        .anyMatch(a -> this.failedTypes.contains(SLURMJobStatusType.valueOf(a.getStatus())));

                boolean containsFinishedJobs = relevantJobStatsInfos.stream()
                        .anyMatch(a -> this.finishedTypes.contains(SLURMJobStatusType.valueOf(a.getStatus())));

                boolean containsRunningJobs = relevantJobStatsInfos.stream()
                        .anyMatch(a -> this.runningTypes.contains(SLURMJobStatusType.valueOf(a.getStatus())));

                boolean containsNotFinishedJobs = relevantJobStatsInfos.stream()
                        .anyMatch(a -> this.notFinishedTypes.contains(SLURMJobStatusType.valueOf(a.getStatus())));

                Map<String, Long> statusCountsMap = jobStatusSet.parallelStream()
                        .filter(a -> a.getJobId().startsWith(job.getId()))
                        .collect(Collectors.groupingByConcurrent(JobStatusInfo::getStatus, Collectors.counting()));

                log.info("job id: {} - {}", job.getId(), statusCountsMap.toString());

                if (!containsRunningJobs && !containsNotFinishedJobs && !containsFailedJobs && containsFinishedJobs) {
                    jobFinished = true;
                    return;
                }

                if (!containsRunningJobs && !containsNotFinishedJobs && !containsFinishedJobs && containsFailedJobs) {
                    relevantJobStatsInfos.stream()
                            .filter(a -> this.failedTypes.contains(SLURMJobStatusType.valueOf(a.getStatus())))
                            .forEach(a -> log.warn(a.toString()));
                    jobFinished = true;
                    return;
                }

                if (containsRunningJobs || containsNotFinishedJobs) {

                    if (containsFailedJobs) {
                        relevantJobStatsInfos.stream()
                                .filter(a -> this.failedTypes.contains(SLURMJobStatusType.valueOf(a.getStatus())))
                                .forEach(a -> log.warn(a.toString()));
                        if (terminateWhenThereAreFailures) {
                            Executors.newSingleThreadExecutor().submit(new SLURMCancelJobCallable(job.getId()));
                            jobFinished = true;
                            return;
                        }
                    }

                    jobFinished = false;
                    return;

                }

            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            jobFinished = true;
        }

    }

}
