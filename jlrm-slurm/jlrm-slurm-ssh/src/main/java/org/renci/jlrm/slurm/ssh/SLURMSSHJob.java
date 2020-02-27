package org.renci.jlrm.slurm.ssh;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.Range;
import org.renci.jlrm.Job;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@NoArgsConstructor
@Getter
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true, exclude = { "array" })
public class SLURMSSHJob extends Job {

    private static final long serialVersionUID = -4585163763638553836L;

    private String queueName;

    private String project;

    private Long wallTime;

    private Integer hostCount;

    private String constraint;

    private Range<Integer> array;

    private Integer maxRunning;

    private Boolean transferInputs = Boolean.FALSE;

    private Boolean transferExecutable = Boolean.FALSE;

    private List<File> inputFiles = new ArrayList<File>();

    @Builder
    public SLURMSSHJob(String id, String name, Path executable, Path submitFile, Path output, Path error,
            Integer numberOfProcessors, String memory, String disk, long duration, TimeUnit durationTimeUnit,
            String queueName, String project, Long wallTime, Integer hostCount, String constraint, Range<Integer> array,
            Integer maxRunning, Boolean transferInputs, Boolean transferExecutable, List<File> inputFiles) {
        super(id, name, executable, submitFile, output, error, numberOfProcessors, memory, disk, duration,
                durationTimeUnit);
        this.queueName = queueName;
        this.project = project;
        this.wallTime = wallTime;
        this.hostCount = hostCount;
        this.constraint = constraint;
        this.array = array;
        this.maxRunning = maxRunning;
        this.transferInputs = transferInputs != null ? transferInputs : Boolean.FALSE;
        this.transferExecutable = transferExecutable != null ? transferExecutable : Boolean.FALSE;
        this.inputFiles = inputFiles != null ? inputFiles : new ArrayList<File>();
    }

}
