package com.ikaver.aagarwal.hw3.common.util;

import java.io.IOException;
import java.net.ServerSocket;

/**
 * Utility class to find a port to debug on. 
 * Taken from {@link 
 *     https://eclipse.googlesource.com/jdt/eclipse.jdt.debug/
 *     +/6d06d6f327c9ff4398c59a6e899534450413404e/org.eclipse.jdt.launching/
 *     launching/org/eclipse/jdt/launching/SocketUtil.java}
 * 
 * @noinstantiate This class is not intended to be instantiated by clients.
 * @noextend This class is not intended to be subclassed by clients.
 */
public class SocketUtil {
	/**
	 * Returns a free port number on localhost, or -1 if unable to find a free
	 * port.
	 * 
	 * @return a free port number on localhost, or -1 if unable to find a free
	 *         port
	 * @since 3.0
	 */
	public static int findFreePort() {
		ServerSocket socket = null;
		try {
			socket = new ServerSocket(0);
			return socket.getLocalPort();
		} catch (IOException e) {
		} finally {
			if (socket != null) {
				try {
					socket.close();
				} catch (IOException e) {
				}
			}
		}
		return -1;
	}
}