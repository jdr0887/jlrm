package org.renci.jlrm;

import java.io.File;
import java.io.FilenameFilter;
import java.text.Format;
import java.util.Date;
import java.util.concurrent.Callable;

public abstract class AbstractSubmitCallable<T> implements Callable<T> {

    public AbstractSubmitCallable() {
        super();
    }

    protected synchronized File createWorkDirectory(File submitDir, String jobsDirName, String name) {
        File jobsDir = new File(submitDir, jobsDirName);
        Date date = new Date();
        Format formatter = PerThreadDateFormatter.getDateFormatter();
        File datedDir = new File(jobsDir, formatter.format(date));
        File jobSubmitDir = new File(datedDir, name);
        File[] runDirectories = jobSubmitDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                if (name.startsWith("run")) {
                    return true;
                }
                return false;
            }
        });
        File runDir = new File(jobSubmitDir, String.format("run%04d", runDirectories != null
                && runDirectories.length > 0 ? runDirectories.length + 1 : 1));
        runDir.mkdirs();
        return runDir;
    }

    protected synchronized File createWorkDirectory(File submitDir, String name) {
        return createWorkDirectory(submitDir, "jobs", name);
    }

}
