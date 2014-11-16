package com.ikaver.aagarwal.hw3.common.objects;

import java.io.Serializable;


public class KeyValuePair implements Serializable, Comparable<KeyValuePair> {

	private static final long serialVersionUID = -7313864475356045542L;

	private final String key;
	private final String value;

	public KeyValuePair(String key, String value) {
		this.key = key;
		this.value = value;
		
	}
	public String getKey() {
		return key;
	}

	public String getValue() {
		return value;
	}
	public int compareTo(KeyValuePair kp) {
		if (this.getKey().equals(kp)) {
			return getValue().compareTo(kp.getValue());
		} 
		return getKey().compareTo(kp.getKey());
	}
}
