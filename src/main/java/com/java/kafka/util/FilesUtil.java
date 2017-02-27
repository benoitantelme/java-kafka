package com.java.kafka.util;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

import com.java.kafka.data.CitiesIndex;

public class FilesUtil {

	public static void cleanPath(String filepath) throws IOException {
		Path rootPath = Paths.get(filepath);
		Files.walk(rootPath, FileVisitOption.FOLLOW_LINKS).sorted(Comparator.reverseOrder()).map(Path::toFile)
				.forEach(File::delete);
	}
	
	public static Path checkFilePath(String filePath) throws Exception {
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

}
