package org.renci.jlrm;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
@EqualsAndHashCode
public class JobStatusInfo implements Serializable {

    private static final long serialVersionUID = 884205842852424191L;

    private String jobId;

    private String status;

    private String queue;

    private String jobName;

}
