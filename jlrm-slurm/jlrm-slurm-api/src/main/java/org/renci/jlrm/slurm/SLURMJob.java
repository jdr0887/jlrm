package org.renci.jlrm.slurm;

import java.io.File;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.Range;
import org.renci.jlrm.Job;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true, exclude = { "array" })
public class SLURMJob extends Job {

    private static final long serialVersionUID = -2381336994166389859L;

    private String queueName;

    private String project;

    private Long wallTime;

    private Integer hostCount;

    private String constraint;

    private Range<Integer> array;

    private Integer maxRunning;

    @Builder
    public SLURMJob(String id, String name, Path executable, Path submitFile, Path output, Path error,
            Integer numberOfProcessors, String memory, String disk, long duration, TimeUnit durationTimeUnit,
            String queueName, String project, Long wallTime, Integer hostCount, String constraint, Range<Integer> array,
            Integer maxRunning) {
        super(id, name, executable, submitFile, output, error, numberOfProcessors, memory, disk, duration,
                durationTimeUnit);
        this.queueName = queueName;
        this.project = project;
        this.wallTime = wallTime;
        this.hostCount = hostCount;
        this.constraint = constraint;
        this.array = array;
        this.maxRunning = maxRunning;
    }

}
