package com.ikaver.aagarwal.hw3.mrclient.jobmonitor;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class JobMonitorEntryPoint {
  

  public static void main(String [] args) {
    Injector injector = Guice.createInjector(new JobMonitorModule("127.0.0.1", 3000));
    JobMonitorController controller = injector.getInstance(JobMonitorController.class);
    controller.start();
  }

}
