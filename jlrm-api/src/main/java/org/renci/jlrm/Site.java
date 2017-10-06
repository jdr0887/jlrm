package org.renci.jlrm;

import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "Site", propOrder = {})
@XmlRootElement(name = "site")
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

    public Site() {
        super();
    }

    public String getSubmitHost() {
        return submitHost;
    }

    public void setSubmitHost(String submitHost) {
        this.submitHost = submitHost;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public List<Queue> getQueueList() {
        return queueList;
    }

    public void setQueueList(List<Queue> queueList) {
        this.queueList = queueList;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    @Override
    public String toString() {
        return String.format("Site [submitHost=%s, username=%s, name=%s, project=%s]", submitHost, username, name,
                project);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((project == null) ? 0 : project.hashCode());
        result = prime * result + ((submitHost == null) ? 0 : submitHost.hashCode());
        result = prime * result + ((username == null) ? 0 : username.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Site other = (Site) obj;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (project == null) {
            if (other.project != null)
                return false;
        } else if (!project.equals(other.project))
            return false;
        if (submitHost == null) {
            if (other.submitHost != null)
                return false;
        } else if (!submitHost.equals(other.submitHost))
            return false;
        if (username == null) {
            if (other.username != null)
                return false;
        } else if (!username.equals(other.username))
            return false;
        return true;
    }

}
