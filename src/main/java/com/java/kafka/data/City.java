package com.java.kafka.data;

public class City {

	private static final String COMMA = ",";
	private String cityName;
	private String country;
	private double lat;
	private double longi;

	public City(String cityName, double lat, double longi, String country) {
		super();
		this.cityName = cityName;
		this.country = country;
		this.lat = lat;
		this.longi = longi;
	}

	public String getCityName() {
		return cityName;
	}

	public String getCountry() {
		return country;
	}

	public double getLat() {
		return lat;
	}

	public double getLongi() {
		return longi;
	}

	@Override
	public String toString() {
		return cityName + COMMA + lat + COMMA + longi + COMMA + country;
	}

	public static City fromString(String s) {
		String[] splitted = s.split(",");

		return new City(splitted[0], Double.parseDouble(splitted[1]), Double.parseDouble(splitted[2]), splitted[3]);
	}

}
