package org.renci.jlrm;

import java.io.File;
import java.util.Date;
import java.util.UUID;

import org.apache.commons.lang3.time.DateFormatUtils;

public class IOUtils {

    public static File createWorkDirectory(File submitDir, String name) {
        File datedDir = new File(submitDir, DateFormatUtils.ISO_DATE_FORMAT.format(new Date()));
        File namedDir = new File(datedDir, name);
        File runDir = new File(namedDir, UUID.randomUUID().toString());
        runDir.mkdirs();
        return runDir;
    }

}
