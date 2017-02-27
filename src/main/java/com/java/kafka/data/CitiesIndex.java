package com.java.kafka.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import com.java.kafka.util.FilesUtil;

public class CitiesIndex {

	private List<City> index;

	public CitiesIndex(String tubeNetworkDescriptionFilePath) throws Exception {
		super();

		Path filePath = FilesUtil.checkFilePath(tubeNetworkDescriptionFilePath);

		try (InputStream is = new FileInputStream(new File(filePath.toString()));
				BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
			index = reader.lines().skip(1).map(line -> City.fromString(line)).collect(Collectors.toList());
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (index.isEmpty())
			throw new Exception("Index is empty after creation from file: " + filePath);
	}

	public int getIndexSize() {
		return index.size();
	}

	public City getIndexElement(int n) {
		return index.get(n);
	}

}
