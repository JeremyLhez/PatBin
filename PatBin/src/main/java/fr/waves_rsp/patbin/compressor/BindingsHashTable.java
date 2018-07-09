package fr.waves_rsp.patbin.compressor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * The binding Table of PatBin algorithm.
 * 
 * @author Jeremy Lhez
 *
 */
public class BindingsHashTable {
	private static int increment = 0;
	private Map<String, Integer> uriBindings;
	private Map<Integer, String> bindingsURI;

	/**
	 * Standard constructor.
	 */
	public BindingsHashTable() {
		this.uriBindings = new HashMap<String, Integer>();
		this.bindingsURI = new HashMap<Integer, String>();
	}

	/**
	 * Constructor to fill the binding table as you wish.
	 * 
	 * @param map
	 *            The map to initialize the binding table.
	 */
	public BindingsHashTable(Map<String, Integer> map) {
		Integer binding;

		this.uriBindings = new HashMap<String, Integer>();
		this.bindingsURI = new HashMap<Integer, String>();

		for (String key : map.keySet()) {
			binding = map.get(key);
			this.uriBindings.put(key, binding);
			this.bindingsURI.put(binding, key);
		}
	}

	/**
	 * Completes the binding table, giving identifiers to the new properties
	 * found in the triples.
	 * 
	 * @param triples
	 *            A list of triples, one by line.
	 */
	public void completeHashTable(String triples) {
		String[] splitedTriples = triples.split(System.getProperty("line.separator"));

		for (String triple : splitedTriples) {
			String[] splitedTriple = triple.split(" ");

			if (!uriBindings.containsKey(splitedTriple[1])) {
				uriBindings.put(splitedTriple[1], increment);
				bindingsURI.put(increment, splitedTriple[1]);
				increment++;
			}
		}
	}

	public Integer get(String predicate) {
		return uriBindings.get(predicate);
	}

	public String get(int binding) {
		return bindingsURI.get(binding);
	}

	public String toString() {
		StringBuilder builder = new StringBuilder();

		for (String s : uriBindings.keySet())
			builder.append(s + " == " + uriBindings.get(s) + System.getProperty("line.separator"));

		return builder.toString();
	}

	/**
	 * Load the binding table from a file. The file must contain, on each line,
	 * the uri of a predicate, and its encoding after a space. No matter what
	 * there is after. Blank lines are ignored.
	 * 
	 * @param f
	 *            The file to be processed in order to build the table.
	 */
	public void loadFromFile(File f) {
		try {
			FileReader fr = new FileReader(f);
			BufferedReader br = new BufferedReader(fr);
			String line;

			while ((line = br.readLine()) != null) {
				if (line.isEmpty())
					continue;

				String[] splitLine = line.split(" ");
				int id = Integer.parseInt(splitLine[1]);

				this.bindingsURI.put(id, splitLine[0]);
				this.uriBindings.put(splitLine[0], id);
			}

			br.close();
		} catch (FileNotFoundException e) {
			System.out.println(f.getAbsolutePath() + " cannot be found.");
		} catch (IOException e) {
			System.out.println(f.getAbsolutePath() + " cannot be read.");
		}
	}
}
