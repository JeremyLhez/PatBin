package fr.waves_rsp.patbin.util;

public class TripleManager {

	/**
	 * Returns the object of a triple. This method is necessary because a simple
	 * split(" ") doesn't always work with String literals.
	 * 
	 * @param triple
	 * @return
	 */
	public static String getObject(String triple) {
		String[] splitedTriple = triple.split(" ");

		if (splitedTriple.length > 3)
			return triple.split("\"")[1];
		else
			return splitedTriple[2];
	}

}
