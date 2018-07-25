package org.renci.jlrm.lsf.ssh;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.renci.jlrm.Job;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.Singular;
import lombok.ToString;

@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
@Getter
@Setter
@ToString(callSuper = true)
public class LSFSSHJob extends Job {

    private static final long serialVersionUID = 5169666827892951127L;

    private String queueName;

    private String project;

    private Long wallTime;

    private Integer hostCount;

    private Boolean transferInputs = Boolean.FALSE;

    private Boolean transferExecutable = Boolean.FALSE;

    @Singular
    private List<File> inputFiles;

    @Builder
    public LSFSSHJob(String id, String name, File executable, File submitFile, File output, File error,
            Integer numberOfProcessors, String memory, String disk, long duration, TimeUnit durationTimeUnit,
            String queueName, String project, Long wallTime, Integer hostCount, Boolean transferInputs,
            Boolean transferExecutable, List<File> inputFiles) {
        super(id, name, executable, submitFile, output, error, numberOfProcessors, memory, disk, duration,
                durationTimeUnit);
        this.queueName = queueName;
        this.project = project;
        this.wallTime = wallTime;
        this.hostCount = hostCount;
        this.transferInputs = transferInputs;
        this.transferExecutable = transferExecutable;
        this.inputFiles = inputFiles;
    }

}
