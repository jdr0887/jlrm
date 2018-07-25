package org.renci.jlrm;

import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "Site", propOrder = {})
@XmlRootElement(name = "site")
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Builder
@ToString(exclude = { "queueList" })
@EqualsAndHashCode(exclude = { "queueList" })
public class Site {

    private String submitHost;

    private String username;

    private String name;

    private String project;

    private List<Queue> queueList;

    public Site(String submitHost, String username) {
        super();
        this.submitHost = submitHost;
        this.username = username;
    }

    public Site(String submitHost, String username, String name, String project) {
        super();
        this.submitHost = submitHost;
        this.username = username;
        this.name = name;
        this.project = project;
    }

}
