package org.renci.jlrm.slurm.cli;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

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
public class SLURMJobStopMonitor implements Runnable {

    private ScheduledExecutorService scheduler;

    private SLURMJobStartMonitor startCondorMonitor;

    private ScheduledFuture<?> startMonitorFuture;

    @Override
    public void run() {
        if (startCondorMonitor.getJobFinished()) {
            log.debug("Shutting down scheduler.");
            startMonitorFuture.cancel(true);
            scheduler.shutdownNow();
        }
    }

}
