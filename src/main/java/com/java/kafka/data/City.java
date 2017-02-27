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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((cityName == null) ? 0 : cityName.hashCode());
		result = prime * result + ((country == null) ? 0 : country.hashCode());
		long temp;
		temp = Double.doubleToLongBits(lat);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(longi);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		City other = (City) obj;
		if (cityName == null) {
			if (other.cityName != null)
				return false;
		} else if (!cityName.equals(other.cityName))
			return false;
		if (country == null) {
			if (other.country != null)
				return false;
		} else if (!country.equals(other.country))
			return false;
		if (Double.doubleToLongBits(lat) != Double.doubleToLongBits(other.lat))
			return false;
		if (Double.doubleToLongBits(longi) != Double.doubleToLongBits(other.longi))
			return false;
		return true;
	}

}
