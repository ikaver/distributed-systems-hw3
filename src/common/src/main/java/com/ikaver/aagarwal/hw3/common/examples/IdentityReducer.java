package com.ikaver.aagarwal.hw3.common.examples;

import java.util.List;

import com.ikaver.aagarwal.hw3.common.mrcollector.ICollector;
import com.ikaver.aagarwal.hw3.common.workers.IReducer;

public class IdentityReducer implements IReducer {

  public void reduce(ICollector collector, String key, List<String> values) {
    collector.collect(key, "");
  }

}
