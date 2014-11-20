package com.ikaver.aagarwal.hw3.mrmap;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

import com.ikaver.aagarwal.hw3.common.dfs.FileUtil;
import com.ikaver.aagarwal.hw3.common.mrmap.IMapOutputCollector;
import com.ikaver.aagarwal.hw3.common.objects.KeyValuePair;
import com.ikaver.aagarwal.hw3.common.util.FileOperationsUtil;

public class MapOutputCollector implements IMapOutputCollector {

	private final Logger LOG = Logger.getLogger(MapOutputCollector.class);

	private final List<KeyValuePair> data;

	public MapOutputCollector() {
		this.data = new ArrayList<KeyValuePair>();
	}

	public void collect(String key, String value) {
		KeyValuePair p = new KeyValuePair(key, value);
		data.add(p);
	}

	// TODO(ankit): Sort the data before storing.
	public String flush() {
		String path = FileOperationsUtil.getRandomStringForLocalFile()
				+ ".out";

		try {
			ObjectOutputStream os = new ObjectOutputStream(
					new FileOutputStream(path));

            // Sorting phase.
			Collections.sort(data);

			// Write the data list to the file.
			os.writeObject(data);

			os.flush();
			os.close();
			FileUtil.changeFilePermission(path);
			// Clear the data node since there are no more entries to be
			// written.
			data.clear();

		} catch (FileNotFoundException e) {
			LOG.fatal("unable to create the random file" + path);
			return null;
		} catch (IOException e) {
			LOG.fatal("Error writing to the file" + path);
			return null;
		}

		return path;
	}
}
