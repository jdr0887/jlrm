package org.renci.jlrm.condor;

import static org.renci.jlrm.condor.ClassAdvertisementType.BOOLEAN;
import static org.renci.jlrm.condor.ClassAdvertisementType.EXPRESSION;
import static org.renci.jlrm.condor.ClassAdvertisementType.INTEGER;
import static org.renci.jlrm.condor.ClassAdvertisementType.STRING;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

public class ClassAdvertisementFactory {

    private static final Map<String, ClassAdvertisement> classAdvertismentMap = new HashMap<String, ClassAdvertisement>();

    public static final String CLASS_AD_KEY_STREAM_ERROR = "stream_error";

    public static final String CLASS_AD_KEY_STREAM_OUTPUT = "stream_output";

    public static final String CLASS_AD_KEY_GET_ENV = "get_env";

    public static final String CLASS_AD_KEY_COPY_TO_SPOOL = "copy_to_spool";

    public static final String CLASS_AD_KEY_NOTIFICATION = "notification";

    public static final String CLASS_AD_KEY_SHOULD_TRANSFER_FILES = "ShouldTransferFiles";

    public static final String CLASS_AD_KEY_WHEN_TO_TRANSFER_OUTPUT = "when_to_transfer_output";

    public static final String CLASS_AD_KEY_TRANSFER_EXECUTABLE = "transfer_executable";

    public static final String CLASS_AD_KEY_QUEUE = "queue";

    public static final String CLASS_AD_KEY_ARGUMENTS = "arguments";

    public static final String CLASS_AD_KEY_PERIODIC_RELEASE = "periodic_release";

    public static final String CLASS_AD_KEY_PERIODIC_REMOVE = "periodic_remove";

    public static final String CLASS_AD_KEY_ON_EXIT_HOLD = "on_exit_hold";

    public static final String CLASS_AD_KEY_ON_EXIT_REMOVE = "on_exit_remove";

    public static final String CLASS_AD_KEY_TRANSFER_ERROR = "transfer_error";

    public static final String CLASS_AD_KEY_TRANSFER_OUTPUT = "transfer_output";

    public static final String CLASS_AD_KEY_TRANSFER_INPUT_FILES = "transfer_input_files";

    public static final String CLASS_AD_KEY_TRANSFER_OUTPUT_FILES = "transfer_output_files";

    public static final String CLASS_AD_KEY_EXECUTABLE = "executable";

    public static final String CLASS_AD_KEY_UNIVERSE = "universe";

    public static final String CLASS_AD_KEY_GLOBUS_RSL = "globusrsl";

    public static final String CLASS_AD_KEY_GRID_RESOURCE = "grid_resource";

    public static final String CLASS_AD_KEY_X509_USER_PROXY = "x509userproxy";

    public static final String CLASS_AD_KEY_REMOTE_INITIAL_DIR = "remote_initialdir";

    public static final String CLASS_AD_KEY_INITIAL_DIR = "initialdir";

    public static final String CLASS_AD_KEY_OUTPUT = "output";

    public static final String CLASS_AD_KEY_ERROR = "error";

    public static final String CLASS_AD_KEY_LOG = "log";

    public static final String CLASS_AD_KEY_REQUEST_CPUS = "request_cpus";

    public static final String CLASS_AD_KEY_REQUEST_MEMORY = "request_memory";

    public static final String CLASS_AD_KEY_REQUEST_DISK = "request_disk";

    public static final String CLASS_AD_KEY_REQUIREMENTS = "requirements";

    public static final String CLASS_AD_KEY_JOB_STATUS = "JobStatus";

    public static final String CLASS_AD_KEY_PRIORITY = "priority";

    static {

        classAdvertismentMap.put(CLASS_AD_KEY_PRIORITY, new ClassAdvertisement(CLASS_AD_KEY_PRIORITY, INTEGER, "0"));

        classAdvertismentMap.put(CLASS_AD_KEY_STREAM_ERROR, new ClassAdvertisement(CLASS_AD_KEY_STREAM_ERROR, BOOLEAN,
                Boolean.FALSE.toString()));

        classAdvertismentMap.put(CLASS_AD_KEY_STREAM_OUTPUT, new ClassAdvertisement(CLASS_AD_KEY_STREAM_OUTPUT,
                BOOLEAN, Boolean.FALSE.toString()));

        classAdvertismentMap.put(CLASS_AD_KEY_GET_ENV, new ClassAdvertisement(CLASS_AD_KEY_GET_ENV, BOOLEAN,
                Boolean.TRUE.toString()));

        classAdvertismentMap.put(CLASS_AD_KEY_COPY_TO_SPOOL, new ClassAdvertisement(CLASS_AD_KEY_COPY_TO_SPOOL,
                BOOLEAN, Boolean.TRUE.toString()));

        classAdvertismentMap.put(CLASS_AD_KEY_NOTIFICATION, new ClassAdvertisement(CLASS_AD_KEY_NOTIFICATION,
                EXPRESSION, "NEVER"));

        classAdvertismentMap.put(CLASS_AD_KEY_TRANSFER_EXECUTABLE, new ClassAdvertisement(
                CLASS_AD_KEY_TRANSFER_EXECUTABLE, EXPRESSION, Boolean.FALSE.toString()));

        classAdvertismentMap.put(CLASS_AD_KEY_QUEUE, new ClassAdvertisement(CLASS_AD_KEY_QUEUE, EXPRESSION, ""));

        classAdvertismentMap.put(CLASS_AD_KEY_ARGUMENTS, new ClassAdvertisement(CLASS_AD_KEY_ARGUMENTS, STRING, ""));

        classAdvertismentMap.put(CLASS_AD_KEY_PERIODIC_RELEASE, new ClassAdvertisement(CLASS_AD_KEY_PERIODIC_RELEASE,
                BOOLEAN, Boolean.FALSE.toString()));

        classAdvertismentMap.put(CLASS_AD_KEY_PERIODIC_REMOVE, new ClassAdvertisement(CLASS_AD_KEY_PERIODIC_REMOVE,
                EXPRESSION, Boolean.FALSE.toString()));

        classAdvertismentMap.put(CLASS_AD_KEY_ON_EXIT_HOLD, new ClassAdvertisement(CLASS_AD_KEY_ON_EXIT_HOLD,
                EXPRESSION, "(ExitBySignal == True) || (ExitCode != 0)"));

        classAdvertismentMap.put(CLASS_AD_KEY_ON_EXIT_REMOVE, new ClassAdvertisement(CLASS_AD_KEY_ON_EXIT_REMOVE,
                EXPRESSION, "(ExitBySignal == True) && (ExitStatus == 0)"));

        classAdvertismentMap.put(CLASS_AD_KEY_TRANSFER_ERROR, new ClassAdvertisement(CLASS_AD_KEY_TRANSFER_ERROR,
                BOOLEAN, Boolean.TRUE.toString()));

        classAdvertismentMap.put(CLASS_AD_KEY_TRANSFER_OUTPUT, new ClassAdvertisement(CLASS_AD_KEY_TRANSFER_OUTPUT,
                BOOLEAN, Boolean.TRUE.toString()));

        classAdvertismentMap.put(CLASS_AD_KEY_TRANSFER_OUTPUT_FILES, new ClassAdvertisement(
                CLASS_AD_KEY_TRANSFER_OUTPUT_FILES, EXPRESSION, ""));

        classAdvertismentMap.put(CLASS_AD_KEY_TRANSFER_INPUT_FILES, new ClassAdvertisement(
                CLASS_AD_KEY_TRANSFER_INPUT_FILES, EXPRESSION, ""));

        classAdvertismentMap.put(CLASS_AD_KEY_EXECUTABLE, new ClassAdvertisement(CLASS_AD_KEY_EXECUTABLE, EXPRESSION,
                ""));

        classAdvertismentMap.put(CLASS_AD_KEY_UNIVERSE, new ClassAdvertisement(CLASS_AD_KEY_UNIVERSE, EXPRESSION,
                UniverseType.VANILLA.toString().toLowerCase()));

        classAdvertismentMap.put(CLASS_AD_KEY_GLOBUS_RSL, new ClassAdvertisement(CLASS_AD_KEY_GLOBUS_RSL, EXPRESSION,
                ""));

        classAdvertismentMap.put(CLASS_AD_KEY_GRID_RESOURCE, new ClassAdvertisement(CLASS_AD_KEY_GRID_RESOURCE,
                EXPRESSION, ""));

        classAdvertismentMap.put(CLASS_AD_KEY_X509_USER_PROXY, new ClassAdvertisement(CLASS_AD_KEY_X509_USER_PROXY,
                EXPRESSION, ""));

        classAdvertismentMap.put(CLASS_AD_KEY_INITIAL_DIR, new ClassAdvertisement(CLASS_AD_KEY_INITIAL_DIR, EXPRESSION,
                ""));

        classAdvertismentMap.put(CLASS_AD_KEY_REMOTE_INITIAL_DIR, new ClassAdvertisement(
                CLASS_AD_KEY_REMOTE_INITIAL_DIR, EXPRESSION, ""));

        classAdvertismentMap.put(CLASS_AD_KEY_OUTPUT, new ClassAdvertisement(CLASS_AD_KEY_OUTPUT, EXPRESSION, ""));

        classAdvertismentMap.put(CLASS_AD_KEY_ERROR, new ClassAdvertisement(CLASS_AD_KEY_ERROR, EXPRESSION, ""));

        classAdvertismentMap.put(CLASS_AD_KEY_LOG, new ClassAdvertisement(CLASS_AD_KEY_LOG, EXPRESSION, ""));

        classAdvertismentMap.put(CLASS_AD_KEY_REQUEST_CPUS, new ClassAdvertisement(CLASS_AD_KEY_REQUEST_CPUS, INTEGER,
                "1"));

        classAdvertismentMap.put(CLASS_AD_KEY_REQUEST_MEMORY, new ClassAdvertisement(CLASS_AD_KEY_REQUEST_MEMORY,
                INTEGER, "1024"));

        classAdvertismentMap.put(CLASS_AD_KEY_REQUEST_DISK, new ClassAdvertisement(CLASS_AD_KEY_REQUEST_DISK, INTEGER,
                "20GB"));

        classAdvertismentMap.put(CLASS_AD_KEY_REQUIREMENTS, new ClassAdvertisement(CLASS_AD_KEY_REQUIREMENTS,
                EXPRESSION, ""));

        classAdvertismentMap.put(CLASS_AD_KEY_SHOULD_TRANSFER_FILES, new ClassAdvertisement(
                CLASS_AD_KEY_SHOULD_TRANSFER_FILES, EXPRESSION, "YES"));

        classAdvertismentMap.put(CLASS_AD_KEY_WHEN_TO_TRANSFER_OUTPUT, new ClassAdvertisement(
                CLASS_AD_KEY_WHEN_TO_TRANSFER_OUTPUT, EXPRESSION, "ON_EXIT"));

    }

    public static ClassAdvertisement getClassAd(String key) {
        return classAdvertismentMap.get(key);
    }

    public static List<ClassAdvertisement> parse(String line) {
        List<ClassAdvertisement> ret = new LinkedList<ClassAdvertisement>();
        if (line.indexOf(',') != -1) {
            String[] keyValuePairArray = line.split(",");
            for (String keyValuePair : keyValuePairArray) {
                int idx = keyValuePair.indexOf('=');
                if (idx != -1) {
                    String key = keyValuePair.substring(0, idx);
                    String value = keyValuePair.substring(idx + 1);
                    ret.add(new ClassAdvertisement(key.trim(), ClassAdvertisementType.EXPRESSION, value.trim()));
                }
            }
        }
        return ret;
    }

    public static Set<ClassAdvertisement> getDefaultClassAds() {
        Set<ClassAdvertisement> ret = new HashSet<ClassAdvertisement>();
        // ret.add(getClassAd(CLASS_AD_KEY_ARGUMENTS));
        // ret.add(getClassAd(CLASS_AD_KEY_REQUIREMENTS));
        ret.add(getClassAd(CLASS_AD_KEY_COPY_TO_SPOOL));
        // ret.add(getClassAd(CLASS_AD_KEY_GET_ENV));
        ret.add(getClassAd(CLASS_AD_KEY_NOTIFICATION));
        ret.add(getClassAd(CLASS_AD_KEY_STREAM_ERROR));
        ret.add(getClassAd(CLASS_AD_KEY_STREAM_OUTPUT));
        ret.add(getClassAd(CLASS_AD_KEY_PRIORITY));
        // ret.add(getClassAd(CLASS_AD_KEY_TRANSFER_EXECUTABLE));
        ret.add(getClassAd(CLASS_AD_KEY_TRANSFER_ERROR));
        ret.add(getClassAd(CLASS_AD_KEY_TRANSFER_OUTPUT));
        ret.add(getClassAd(CLASS_AD_KEY_PERIODIC_RELEASE));
        ret.add(getClassAd(CLASS_AD_KEY_PERIODIC_REMOVE));
        ret.add(getClassAd(CLASS_AD_KEY_ON_EXIT_HOLD));
        // ret.add(getClassAd(CLASS_AD_KEY_ON_EXIT_REMOVE));
        ret.add(getClassAd(CLASS_AD_KEY_UNIVERSE));
        ret.add(getClassAd(CLASS_AD_KEY_REQUEST_CPUS));
        ret.add(getClassAd(CLASS_AD_KEY_REQUEST_MEMORY));
        ret.add(getClassAd(CLASS_AD_KEY_REQUEST_DISK));
        ret.add(getClassAd(CLASS_AD_KEY_SHOULD_TRANSFER_FILES));
        ret.add(getClassAd(CLASS_AD_KEY_WHEN_TO_TRANSFER_OUTPUT));
        return ret;
    }

    public static Set<ClassAdvertisement> getGridJobClassAds(Map<String, String> rslAttributes, String gridResource,
            File proxy, String remoteInitialDir) {

        Set<ClassAdvertisement> ret = getDefaultClassAds();
        try {

            ClassAdvertisement classAd = getClassAd(CLASS_AD_KEY_UNIVERSE);
            if (ret.contains(classAd)) {
                ret.remove(classAd);
            }
            classAd = classAd.clone();
            classAd.setValue(UniverseType.GRID.toString().toLowerCase());
            ret.add(classAd);

            classAd = getClassAd(CLASS_AD_KEY_GLOBUS_RSL);
            if (ret.contains(classAd)) {
                ret.remove(classAd);
            }
            classAd = classAd.clone();
            StringBuilder sb = new StringBuilder();
            for (String key : rslAttributes.keySet()) {
                String value = rslAttributes.get(key);
                sb.append(String.format("(%s=%s)", key, value));
            }
            classAd.setValue(sb.toString());
            ret.add(classAd);

            classAd = getClassAd(CLASS_AD_KEY_GRID_RESOURCE);
            if (ret.contains(classAd)) {
                ret.remove(classAd);
            }
            classAd = classAd.clone();
            classAd.setValue(gridResource);
            ret.add(classAd);

            if (proxy != null) {
                classAd = getClassAd(CLASS_AD_KEY_X509_USER_PROXY);
                if (ret.contains(classAd)) {
                    ret.remove(classAd);
                }
                classAd = classAd.clone();
                classAd.setValue(proxy.getAbsolutePath());
                ret.add(classAd);
            }

            if (StringUtils.isNotEmpty(remoteInitialDir)) {
                classAd = getClassAd(CLASS_AD_KEY_REMOTE_INITIAL_DIR);
                if (ret.contains(classAd)) {
                    ret.remove(classAd);
                }
                classAd = classAd.clone();
                classAd.setValue(remoteInitialDir);
                ret.add(classAd);
            }

        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }

        return ret;
    }
}
