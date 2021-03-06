package org.renci.jlrm.sge.ssh;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.renci.jlrm.Job;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
@Getter
@Setter
@ToString(callSuper = true)
public class SGESSHJob extends Job {

    private static final long serialVersionUID = -3637899659269488349L;

    private String queueName;

    private String project;

    private Long wallTime;

    private Integer hostCount;

    private Boolean transferInputs = Boolean.FALSE;

    private Boolean transferExecutable = Boolean.FALSE;

    private List<File> inputFiles = new ArrayList<File>();

    @Builder
    public SGESSHJob(String id, String name, Path executable, Path submitFile, Path output, Path error,
            Integer numberOfProcessors, Integer memory, String disk, long duration, TimeUnit durationTimeUnit,
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
