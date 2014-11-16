package com.ikaver.aagarwal.hw3.common.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.ikaver.aagarwal.hw3.common.definitions.Definitions;
import com.ikaver.aagarwal.hw3.common.dfs.FileUtil;

public class FileOperationsUtil {

	private static final Logger LOG = Logger.getLogger(FileOperationsUtil.class);

	public static String storeLocalFile(byte[] data) {
		String localfp = FileOperationsUtil.getRandomStringForLocalFile();
		try {
			FileOutputStream os;
			os = new FileOutputStream(new File(localfp));
			os.write(data);
			os.close();
			FileUtil.changeFilePermission(localfp);
			return localfp;
		} catch (FileNotFoundException e) {
			LOG.warn("error writing to the file." + localfp, e);
		} catch (IOException e) {
			LOG.warn("IO exception while writing to the file.", e);
		}
		return null;
	}

	public static String getRandomStringForLocalFile() {
		File file;
		String path;
		do {
			path = Definitions.LOCAL_FS_BASE_DIRECTORY
				+ UUID.randomUUID().toString();
			file = new  File(path);
		} while(file.exists());
		return path;
	}
}
