package com.ikaver.aagarwal.hw3.mrmap;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.ikaver.aagarwal.hw3.common.mrmap.IMapOutputCollector;
import com.ikaver.aagarwal.hw3.common.objects.KeyValuePair;
import com.ikaver.aagarwal.hw3.common.util.StringUtil;

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
		String path;
		File file;

		// Generate a random file name.
		do {
			path = getRandomFileName();
			file = new File(path);
		} while (file.exists());

		try {
			ObjectOutputStream os = new ObjectOutputStream(
					new FileOutputStream(file));

			for (int i = 0; i < data.size(); i++) {
				os.writeObject(data.get(i));
			}

			os.flush();
			os.close();

			// Clear the data node since there are no more entries to be
			// written.
			data.clear();

		} catch (FileNotFoundException e) {
			LOG.fatal("unable to create the random file" + file);
			return null;
		} catch (IOException e) {
			LOG.fatal("Error writing to the file" + file);
			return null;
		}

		return path;
	}

	private String getRandomFileName() {
		return StringUtil.getRandomString();
	}

}