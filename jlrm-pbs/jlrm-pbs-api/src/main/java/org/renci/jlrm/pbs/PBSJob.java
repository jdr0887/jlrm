package org.renci.jlrm.pbs;

import org.renci.jlrm.Job;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Builder
@EqualsAndHashCode(callSuper = true)
@Getter
@Setter
@ToString(callSuper = true)
public class PBSJob extends Job {

    private static final long serialVersionUID = -47993939893782293L;

    @Builder.Default
    private String queueName = "default";

    private String project;

    private Long wallTime;

    private Integer hostCount;

}
