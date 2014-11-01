package com.ikaver.aagarwal.hw3.mrslave.main;

import com.ikaver.aagarwal.hw3.common.slave.IMRTaskManager;

/**
 * A task manager manages a "slave" node. Following are the responsibilities of
 * a task manager.
 * 1. Accepts a jar and forks out separate jvms for running mapper and reducer
 *   tasks.
 * 2. Periodically updates master with the status of the map reduce
 *   job assigned to it.
 */
public class MRTaskManager implements IMRTaskManager {

}
