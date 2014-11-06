package com.ikaver.aagarwal.hw3.common.workers;

import java.io.Serializable;

public class MapperOutput<K extends Serializable & Comparable<K>, V extends Serializable> implements Serializable {

  private static final long serialVersionUID = 9132485412222294379L;
  
  private final K key;
  private final V value;
  
  public MapperOutput(K key, V value) {
    this.key = key;
    this.value = value;
  }

  public K getKey() {
    return key;
  }

  public V getValue() {
    return value;
  }
  
}
