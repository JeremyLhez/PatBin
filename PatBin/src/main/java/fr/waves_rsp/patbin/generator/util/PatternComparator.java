package fr.waves_rsp.patbin.generator.util;

import java.util.Comparator;

/**
 * Comparator for the patterns of PatBin.
 * 
 * @author Jeremy Lhez
 *
 */
public class PatternComparator implements Comparator<String> {

	public int compare(String o1, String o2) {
		String[] s1 = o1.replaceAll("\\(", "").replaceAll("\\)", "").split(":");
		String[] s2 = o2.replaceAll("\\(", "").replaceAll("\\)", "").split(":");

		for (int i = 0; i < s1.length; i++)
			if (i < s2.length) {
				int currentID1 = Integer.parseInt(s1[i]);
				int currentID2 = Integer.parseInt(s2[i]);

				if (currentID1 < currentID2)
					return -1;
				if (currentID1 > currentID2)
					return 1;
			}

		return 0;
	}

}
