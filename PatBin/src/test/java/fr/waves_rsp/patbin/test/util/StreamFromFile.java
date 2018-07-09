package fr.waves_rsp.patbin.test.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StreamFromFile {
	public static final String SEP = System.getProperty("line.separator");

	public static List<String> extractSamplesFromFile(String file) {
		List<String> samples = new ArrayList<String>();

		try {
			FileReader fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);
			String line;
			StringBuilder sample = new StringBuilder("");

			while ((line = br.readLine()) != null) {
				if (line.equals("")) {
					samples.add(sample.toString());
					sample = new StringBuilder("");
				} else
					sample.append(line + SEP);
			}
			
			samples.add(sample.toString());
			br.close();
		} catch (FileNotFoundException e) {
			System.out.println(file + " canno be found.");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("Impossible to read the file " + file);
			e.printStackTrace();
		}

		return samples;
	}

}
