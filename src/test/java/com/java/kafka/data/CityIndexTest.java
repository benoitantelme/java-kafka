package com.java.kafka.data;

import static org.junit.Assert.*;

import org.junit.Test;

public class CityIndexTest {

	@Test
	public void loadingIndexTest() throws Exception {
		CitiesIndex index = new CitiesIndex("src/main/resources/worldcities.csv");
		assertEquals(7322, index.getIndexSize());
	}

}
