package com.ikaver.aagarwal.hw3.common.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.ikaver.aagarwal.hw3.common.definitions.Definitions;

/**
 * Util class for dealing with local file system operations.
 */
public class LocalFSOperationsUtil {

	private static final Logger LOGGER = Logger
			.getLogger(LocalFSOperationsUtil.class);

	public static String storeLocalFile(byte[] data, String extension) {
		String localfp = LocalFSOperationsUtil.getRandomStringForLocalFile();
		try {
			FileOutputStream os;
			os = new FileOutputStream(new File(localfp));
			os.write(data);
			os.close();
			changeFilePermission(localfp);
			return localfp;
		} catch (FileNotFoundException e) {
			LOGGER.warn("error writing to the file." + localfp, e);
		} catch (IOException e) {
			LOGGER.warn("IO exception while writing to the file.", e);
		}
		return null;
	}

	public static String getRandomStringForLocalFile() {
		File file;
		String path;
		do {
			path = Definitions.LOCAL_FS_BASE_DIRECTORY + "/"
					+ UUID.randomUUID().toString();
			file = new File(path);
		} while (file.exists());
		return path;
	}

	/**
	 * Sets the file permission for a file as "777".
	 */
	public static void changeFilePermission(String filePathForFile) {
		try {
			Runtime.getRuntime().exec("chmod 777 " + filePathForFile);
		} catch (IOException e) {
			LOGGER.warn(
					"error changing file permission for " + filePathForFile, e);
		}
	}

}
