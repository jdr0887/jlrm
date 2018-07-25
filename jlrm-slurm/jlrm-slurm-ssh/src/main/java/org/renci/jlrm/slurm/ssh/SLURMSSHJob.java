package org.renci.jlrm.slurm.ssh;

import java.io.File;
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
@EqualsAndHashCode(callSuper = true, exclude = { "array" })
@Getter
@Setter
@ToString(callSuper = true)
public class SLURMSSHJob extends Job {

    private static final long serialVersionUID = -4585163763638553836L;

    private String queueName;

    private String project;

    private Long wallTime;

    private Integer hostCount;

    private String constraint;

    private Range<Integer> array;

    private Boolean transferInputs = Boolean.FALSE;

    private Boolean transferExecutable = Boolean.FALSE;

    private List<File> inputFiles = new ArrayList<File>();

    @Builder
    public SLURMSSHJob(String id, String name, File executable, File submitFile, File output, File error,
            Integer numberOfProcessors, String memory, String disk, long duration, TimeUnit durationTimeUnit,
            String queueName, String project, Long wallTime, Integer hostCount, String constraint, Range<Integer> array,
            Boolean transferInputs, Boolean transferExecutable, List<File> inputFiles) {
        super(id, name, executable, submitFile, output, error, numberOfProcessors, memory, disk, duration,
                durationTimeUnit);
        this.queueName = queueName;
        this.project = project;
        this.wallTime = wallTime;
        this.hostCount = hostCount;
        this.constraint = constraint;
        this.array = array;
        this.transferInputs = transferInputs;
        this.transferExecutable = transferExecutable;
        this.inputFiles = inputFiles;
    }

}
