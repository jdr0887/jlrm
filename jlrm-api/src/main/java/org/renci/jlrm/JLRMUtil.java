package org.renci.jlrm;

import java.io.File;
import java.util.Date;
import java.util.UUID;

import org.apache.commons.lang3.time.DateFormatUtils;

public class JLRMUtil {

    public static File createWorkDirectory(File submitDir, String name) {
        File datedDir = new File(submitDir, DateFormatUtils.ISO_8601_EXTENDED_DATE_FORMAT.format(new Date()));
        File namedDir = new File(datedDir, name);
        File runDir = new File(namedDir, UUID.randomUUID().toString());
        runDir.mkdirs();
        return runDir;
    }

    public static File createSubmitDirectory(String project) throws Exception {
        File hiddenJLRMDir = new File(System.getProperty("user.home"), ".jlrm");
        File projectDir = new File(hiddenJLRMDir, project);
        File jobsDir = new File(projectDir, "jobs");
        File dateDir = new File(jobsDir, DateFormatUtils.ISO_8601_EXTENDED_DATE_FORMAT.format(new Date()));
        File submitDir = new File(dateDir, UUID.randomUUID().toString());
        submitDir.mkdirs();
        return submitDir;
    }

}
