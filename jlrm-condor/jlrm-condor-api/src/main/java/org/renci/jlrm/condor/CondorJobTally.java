package org.renci.jlrm.condor;

import java.util.Date;

public class CondorJobTally {

    private Date date;

    private Integer done;

    private Integer pre;

    private Integer queued;

    private Integer post;

    private Integer ready;

    private Integer unReady;

    private Integer failed;

    private Integer held;

    public CondorJobTally() {
        super();
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public Integer getDone() {
        return done;
    }

    public void setDone(Integer done) {
        this.done = done;
    }

    public Integer getPre() {
        return pre;
    }

    public void setPre(Integer pre) {
        this.pre = pre;
    }

    public Integer getQueued() {
        return queued;
    }

    public void setQueued(Integer queued) {
        this.queued = queued;
    }

    public Integer getPost() {
        return post;
    }

    public void setPost(Integer post) {
        this.post = post;
    }

    public Integer getReady() {
        return ready;
    }

    public void setReady(Integer ready) {
        this.ready = ready;
    }

    public Integer getUnReady() {
        return unReady;
    }

    public void setUnReady(Integer unReady) {
        this.unReady = unReady;
    }

    public Integer getFailed() {
        return failed;
    }

    public void setFailed(Integer failed) {
        this.failed = failed;
    }

    public Integer getHeld() {
        return held;
    }

    public void setHeld(Integer held) {
        this.held = held;
    }

    @Override
    public String toString() {
        return String
                .format("CondorJobTally [date=%s, done=%s, pre=%s, queued=%s, post=%s, ready=%s, unReady=%s, failed=%s, held=%s]",
                        date, done, pre, queued, post, ready, unReady, failed, held);
    }

}
