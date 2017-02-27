package com.java.kafka.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CitiesIndex {

	private List<City> index;

	public CitiesIndex(String tubeNetworkDescriptionFilePath) throws Exception {
		super();

		Path filePath = checkFilePath(tubeNetworkDescriptionFilePath);

		try (InputStream is = new FileInputStream(new File(filePath.toString()));
				BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
			index = reader.lines().skip(1).map(mapToCity).collect(Collectors.toList());
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (index.isEmpty())
			throw new Exception("Index is empty after creation from file: " + filePath);
	}

	private static Function<String, City> mapToCity = (line) -> {
		return City.fromString(line);
	};

	private Path checkFilePath(String filePath) throws Exception {
		File file = null;
		URL resource = CitiesIndex.class.getClassLoader().getResource(filePath);
		if (resource != null) {
			file = Paths.get(resource.toURI()).toFile();
		} else {
			file = new File(filePath);
		}

		Path cleanFIlePath = Paths.get(file.getAbsolutePath());

		if (!Files.exists(cleanFIlePath) || !Files.isReadable(cleanFIlePath) || Files.isDirectory(cleanFIlePath))
			throw new Exception("Can not read file: " + cleanFIlePath);

		return cleanFIlePath;
	}
	
	public int getIndexSize() {
		return index.size();
	}

	public City getIndexElement(int n) {
		return index.get(n);
	}

}
