package com.ikaver.aagarwal.hw3.common.examples;

import com.ikaver.aagarwal.hw3.common.mrcollector.ICollector;
import com.ikaver.aagarwal.hw3.common.workers.IMapper;

public class GrepMapper implements IMapper {
  
  private static String TARGET_PATTERN = "DSFTW";

  public void map(String record, ICollector collector) {
    String cleanRecord = record.trim();
    if(cleanRecord.contains(TARGET_PATTERN)) {
      collector.collect(cleanRecord, "");
    }
  }

}