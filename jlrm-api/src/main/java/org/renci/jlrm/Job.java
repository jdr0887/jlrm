package org.renci.jlrm;

import java.io.File;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "Job", propOrder = {})
@XmlRootElement(name = "job")
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
@EqualsAndHashCode(exclude = { "durationTimeUnit" })
public class Job implements Serializable {

    private static final long serialVersionUID = 5760998445998689534L;

    @XmlAttribute
    protected String id;

    @XmlAttribute
    protected String name;

    protected File executable;

    protected File submitFile;

    protected File output;

    protected File error;

    protected Integer numberOfProcessors = 1;

    protected String memory = "2048";

    protected String disk;

    protected long duration;

    protected TimeUnit durationTimeUnit;

}
