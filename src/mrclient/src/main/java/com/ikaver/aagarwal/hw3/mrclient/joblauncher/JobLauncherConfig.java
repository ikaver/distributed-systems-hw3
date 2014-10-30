package com.ikaver.aagarwal.hw3.mrclient.joblauncher;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class JobLauncherConfig {
  
  private static Injector injector;
  
  public static void initialize() {
    if(injector == null) injector = Guice.createInjector(new JobLauncherModule());
  }

}
