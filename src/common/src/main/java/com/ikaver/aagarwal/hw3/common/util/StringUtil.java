package com.ikaver.aagarwal.hw3.common.util;

import java.util.UUID;

public class StringUtil {

	public static String getRandomString() {
		return UUID.randomUUID().toString();
	}
}
