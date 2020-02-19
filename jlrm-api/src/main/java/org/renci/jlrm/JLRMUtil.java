package org.renci.jlrm;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.UUID;

import org.apache.commons.lang3.time.DateFormatUtils;

public class JLRMUtil {

    public static Path createWorkDirectory(Path submitDir, String name) throws IOException {
        Path runDir = Paths.get(submitDir.toAbsolutePath().toString(),
                DateFormatUtils.ISO_8601_EXTENDED_DATE_FORMAT.format(new Date()), name, UUID.randomUUID().toString());
        Files.createDirectories(runDir);
        return runDir;
    }

    public static Path createSubmitDirectory(String project) throws IOException {
        Path submitDir = Paths.get(System.getProperty("user.home"), ".jlrm", project, "jobs",
                DateFormatUtils.ISO_8601_EXTENDED_DATE_FORMAT.format(new Date()), UUID.randomUUID().toString());
        Files.createDirectories(submitDir);
        return submitDir;
    }

}
