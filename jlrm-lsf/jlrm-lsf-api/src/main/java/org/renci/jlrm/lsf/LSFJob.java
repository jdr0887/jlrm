package org.renci.jlrm.lsf;

import org.renci.jlrm.Job;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Builder
@Getter
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class LSFJob extends Job {

    private static final long serialVersionUID = 7771309037426251267L;

    private String queueName;

    private String project;

    private Long wallTime;

    private Integer hostCount;

}
