package com.ikaver.aagarwal.hw3.mrmaster.jobmanager.jobvalidator;

import com.ikaver.aagarwal.hw3.common.config.JobConfig;

public interface IJobValidator {
  public boolean isJobValid(JobConfig job);
}
