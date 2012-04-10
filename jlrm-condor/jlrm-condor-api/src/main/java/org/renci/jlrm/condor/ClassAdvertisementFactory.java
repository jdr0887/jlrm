package org.renci.jlrm.condor;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

public class ClassAdvertisementFactory {

	private static final Map<String, ClassAdvertisement> classAdvertismentMap = new HashMap<String, ClassAdvertisement>();

	public static final String CLASS_AD_KEY_STREAM_ERROR = "stream_error";

	public static final String CLASS_AD_KEY_STREAM_OUTPUT = "stream_output";

	public static final String CLASS_AD_KEY_GET_ENV = "get_env";

	public static final String CLASS_AD_KEY_COPY_TO_SPOOL = "copy_to_spool";

	public static final String CLASS_AD_KEY_NOTIFICATION = "notification";

	public static final String CLASS_AD_KEY_TRANSFER_EXECUTABLE = "transfer_executable";

	public static final String CLASS_AD_KEY_QUEUE = "queue";

	public static final String CLASS_AD_KEY_ARGUMENTS = "arguments";

	public static final String CLASS_AD_KEY_PERIODIC_RELEASE = "periodic_release";

	public static final String CLASS_AD_KEY_PERIODIC_REMOVE = "periodic_remove";

	public static final String CLASS_AD_KEY_TRANSFER_ERROR = "transfer_error";

	public static final String CLASS_AD_KEY_TRANSFER_OUTPUT = "transfer_output";

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

	static {

		classAdvertismentMap.put(
				CLASS_AD_KEY_STREAM_ERROR,
				new ClassAdvertisement(CLASS_AD_KEY_STREAM_ERROR,
						ClassAdvertisementType.BOOLEAN, Boolean.FALSE
								.toString()));

		classAdvertismentMap.put(
				CLASS_AD_KEY_STREAM_OUTPUT,
				new ClassAdvertisement(CLASS_AD_KEY_STREAM_OUTPUT,
						ClassAdvertisementType.BOOLEAN, Boolean.FALSE
								.toString()));

		classAdvertismentMap.put(CLASS_AD_KEY_GET_ENV, new ClassAdvertisement(
				CLASS_AD_KEY_GET_ENV, ClassAdvertisementType.BOOLEAN,
				Boolean.TRUE.toString()));

		classAdvertismentMap.put(
				CLASS_AD_KEY_COPY_TO_SPOOL,
				new ClassAdvertisement(CLASS_AD_KEY_COPY_TO_SPOOL,
						ClassAdvertisementType.BOOLEAN, Boolean.FALSE
								.toString()));

		classAdvertismentMap.put(CLASS_AD_KEY_NOTIFICATION,
				new ClassAdvertisement(CLASS_AD_KEY_NOTIFICATION,
						ClassAdvertisementType.EXPRESSION, "NEVER"));

		classAdvertismentMap.put(
				CLASS_AD_KEY_TRANSFER_EXECUTABLE,
				new ClassAdvertisement(CLASS_AD_KEY_TRANSFER_EXECUTABLE,
						ClassAdvertisementType.EXPRESSION, Boolean.FALSE
								.toString()));

		classAdvertismentMap.put(CLASS_AD_KEY_QUEUE, new ClassAdvertisement(
				CLASS_AD_KEY_QUEUE, ClassAdvertisementType.EXPRESSION, ""));

		classAdvertismentMap.put(CLASS_AD_KEY_ARGUMENTS,
				new ClassAdvertisement(CLASS_AD_KEY_ARGUMENTS,
						ClassAdvertisementType.STRING, ""));

		classAdvertismentMap.put(
				CLASS_AD_KEY_PERIODIC_RELEASE,
				new ClassAdvertisement(CLASS_AD_KEY_PERIODIC_RELEASE,
						ClassAdvertisementType.BOOLEAN, Boolean.FALSE
								.toString()));

		classAdvertismentMap.put(CLASS_AD_KEY_PERIODIC_REMOVE,
				new ClassAdvertisement(CLASS_AD_KEY_PERIODIC_REMOVE,
						ClassAdvertisementType.EXPRESSION, "(JobStatus == 5)"));

		classAdvertismentMap
				.put(CLASS_AD_KEY_TRANSFER_ERROR,
						new ClassAdvertisement(CLASS_AD_KEY_TRANSFER_ERROR,
								ClassAdvertisementType.BOOLEAN, Boolean.TRUE
										.toString()));

		classAdvertismentMap
				.put(CLASS_AD_KEY_TRANSFER_OUTPUT,
						new ClassAdvertisement(CLASS_AD_KEY_TRANSFER_OUTPUT,
								ClassAdvertisementType.BOOLEAN, Boolean.TRUE
										.toString()));

		classAdvertismentMap.put(CLASS_AD_KEY_EXECUTABLE,
				new ClassAdvertisement(CLASS_AD_KEY_EXECUTABLE,
						ClassAdvertisementType.EXPRESSION, ""));

		classAdvertismentMap.put(CLASS_AD_KEY_UNIVERSE, new ClassAdvertisement(
				CLASS_AD_KEY_UNIVERSE, ClassAdvertisementType.EXPRESSION,
				UniverseType.VANILLA.toString().toLowerCase()));

		classAdvertismentMap.put(CLASS_AD_KEY_GLOBUS_RSL,
				new ClassAdvertisement(CLASS_AD_KEY_GLOBUS_RSL,
						ClassAdvertisementType.EXPRESSION, ""));

		classAdvertismentMap.put(CLASS_AD_KEY_GRID_RESOURCE,
				new ClassAdvertisement(CLASS_AD_KEY_GRID_RESOURCE,
						ClassAdvertisementType.EXPRESSION, ""));

		classAdvertismentMap.put(CLASS_AD_KEY_X509_USER_PROXY,
				new ClassAdvertisement(CLASS_AD_KEY_X509_USER_PROXY,
						ClassAdvertisementType.EXPRESSION, ""));

		classAdvertismentMap.put(CLASS_AD_KEY_INITIAL_DIR,
				new ClassAdvertisement(CLASS_AD_KEY_INITIAL_DIR,
						ClassAdvertisementType.EXPRESSION, ""));

		classAdvertismentMap.put(CLASS_AD_KEY_REMOTE_INITIAL_DIR,
				new ClassAdvertisement(CLASS_AD_KEY_REMOTE_INITIAL_DIR,
						ClassAdvertisementType.EXPRESSION, ""));

		classAdvertismentMap.put(CLASS_AD_KEY_OUTPUT, new ClassAdvertisement(
				CLASS_AD_KEY_OUTPUT, ClassAdvertisementType.EXPRESSION, ""));

		classAdvertismentMap.put(CLASS_AD_KEY_ERROR, new ClassAdvertisement(
				CLASS_AD_KEY_ERROR, ClassAdvertisementType.EXPRESSION, ""));

		classAdvertismentMap.put(CLASS_AD_KEY_LOG, new ClassAdvertisement(
				CLASS_AD_KEY_LOG, ClassAdvertisementType.EXPRESSION, ""));

		classAdvertismentMap.put(CLASS_AD_KEY_REQUEST_CPUS,
				new ClassAdvertisement(CLASS_AD_KEY_REQUEST_CPUS,
						ClassAdvertisementType.INTEGER, "1"));

		classAdvertismentMap.put(CLASS_AD_KEY_REQUEST_MEMORY,
				new ClassAdvertisement(CLASS_AD_KEY_REQUEST_MEMORY,
						ClassAdvertisementType.INTEGER, "1024"));

		classAdvertismentMap.put(CLASS_AD_KEY_REQUEST_DISK,
				new ClassAdvertisement(CLASS_AD_KEY_REQUEST_DISK,
						ClassAdvertisementType.INTEGER, "10240"));

		classAdvertismentMap.put(CLASS_AD_KEY_REQUIREMENTS,
				new ClassAdvertisement(CLASS_AD_KEY_REQUIREMENTS,
						ClassAdvertisementType.EXPRESSION, ""));

	}

	public static ClassAdvertisement getClassAd(String key) {
		return classAdvertismentMap.get(key);
	}

	public static Set<ClassAdvertisement> getDefaultClassAds() {
		Set<ClassAdvertisement> ret = new HashSet<ClassAdvertisement>();
		// ret.add(getClassAd(CLASS_AD_KEY_ARGUMENTS));
		// ret.add(getClassAd(CLASS_AD_KEY_REQUIREMENTS));
		ret.add(getClassAd(CLASS_AD_KEY_COPY_TO_SPOOL));
		ret.add(getClassAd(CLASS_AD_KEY_GET_ENV));
		ret.add(getClassAd(CLASS_AD_KEY_NOTIFICATION));
		ret.add(getClassAd(CLASS_AD_KEY_STREAM_ERROR));
		ret.add(getClassAd(CLASS_AD_KEY_STREAM_OUTPUT));
		ret.add(getClassAd(CLASS_AD_KEY_TRANSFER_EXECUTABLE));
		ret.add(getClassAd(CLASS_AD_KEY_TRANSFER_ERROR));
		ret.add(getClassAd(CLASS_AD_KEY_TRANSFER_OUTPUT));
		ret.add(getClassAd(CLASS_AD_KEY_PERIODIC_RELEASE));
		ret.add(getClassAd(CLASS_AD_KEY_PERIODIC_REMOVE));
		ret.add(getClassAd(CLASS_AD_KEY_UNIVERSE));
		ret.add(getClassAd(CLASS_AD_KEY_REQUEST_CPUS));
		ret.add(getClassAd(CLASS_AD_KEY_REQUEST_MEMORY));
		ret.add(getClassAd(CLASS_AD_KEY_REQUEST_DISK));
		return ret;
	}

	public static Set<ClassAdvertisement> getGridJobClassAds(
			Map<String, String> rslAttributes, String gridResource, File proxy,
			String remoteInitialDir) {

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
