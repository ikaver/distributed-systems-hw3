package com.ikaver.aagarwal.hw3.common.util;

import java.util.UUID;

import com.ikaver.aagarwal.hw3.common.definitions.Definitions;

public class FileNameUtil {

	public static String getRandomStringForLocalFile() {
		return Definitions.LOCAL_FS_BASE_DIRECTORY + UUID.randomUUID().toString();
	}
}
