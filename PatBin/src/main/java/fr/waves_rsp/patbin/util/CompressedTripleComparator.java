package fr.waves_rsp.patbin.util;

import java.util.Comparator;

/**
 * Compares two triples in a compressed form (property as identifier), using
 * the subject and property. The method tests first the subjects, using
 * String.compareTo method. If the subjects are equal, then the method
 * compares the properties (their integer identifiers).
 */
public class CompressedTripleComparator implements Comparator<String> {

	public CompressedTripleComparator() {
	}

	@Override
	public int compare(String triple1, String triple2) {
		String[] splitedTriple1 = triple1.split(" ");
		String[] splitedTriple2 = triple2.split(" ");
		int subjectComparison = splitedTriple1[0].compareTo(splitedTriple2[0]);

		if (subjectComparison == 0)
			return Integer.parseInt(splitedTriple1[1]) - Integer.parseInt(splitedTriple2[1]);

		return subjectComparison;
	}

}
