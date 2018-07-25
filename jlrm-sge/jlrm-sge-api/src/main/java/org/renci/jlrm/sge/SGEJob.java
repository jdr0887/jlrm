package org.renci.jlrm.sge;

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
public class SGEJob extends Job {

    private static final long serialVersionUID = 2496405773219470965L;

    protected String queueName;

    protected String project;

    protected Long wallTime;

    protected Integer hostCount;

}
