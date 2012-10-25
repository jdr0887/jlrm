package org.renci.jlrm;

import java.io.File;
import java.text.Format;
import java.util.Date;
import java.util.UUID;

public class IOUtils {

    public static File createWorkDirectory(File submitDir, String name) {
        Format formatter = PerThreadDateFormatter.getDateFormatter();
        File datedDir = new File(submitDir, formatter.format(new Date()));
        File namedDir = new File(datedDir, name);
        File runDir = new File(namedDir, UUID.randomUUID().toString());
        runDir.mkdirs();
        return runDir;
    }

}
