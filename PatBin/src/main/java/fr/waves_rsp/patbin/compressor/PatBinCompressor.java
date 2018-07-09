package fr.waves_rsp.patbin.compressor;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.StmtIterator;

import fr.waves_rsp.patbin.PatBin;
import fr.waves_rsp.patbin.util.CompressedTripleComparator;
import fr.waves_rsp.patbin.util.TripleManager;

public class PatBinCompressor {
	private static final String SEPARATOR = System.getProperty("line.separator");

	private BindingsHashTable bindingsTable = new BindingsHashTable();
	// list of triples, sorted, with the predicate replaced by its identifier
	private static Map<String, Map<Integer, List<String>>> triplesMap;
	// binding builder, used for compression
	private static StringBuilder binBuilder;
	// variables used for decompression
	private static List<String> tmpBindings;
	private static int conceptIdentifier;
	private static int predicatePosition;

	/**
	 * Default constructor.
	 */
	public PatBinCompressor() {
		this.bindingsTable = new BindingsHashTable();
	}

	/**
	 * Compresses a RDF stream using PatBin algorithm.
	 * 
	 * @param input
	 *            The stream to be compressed.
	 * @param format
	 *            The format of the stream.
	 * @return The compressed form.
	 */
	public PatBin compressFile(InputStream input, String format) {
		Model jenaModel = ModelFactory.createDefaultModel();
		StringBuilder triplesBuilder = new StringBuilder();
		String pattern = "", binding;

		jenaModel.read(input, null, format);
		StmtIterator statementIterator = jenaModel.listStatements();
		while (statementIterator.hasNext()) {
			Triple triple = statementIterator.nextStatement().asTriple();
			triplesBuilder.append(triple.getSubject().toString() + " " + triple.getPredicate().toString() + " "
					+ triple.getObject().toString());
			if (statementIterator.hasNext())
				triplesBuilder.append(SEPARATOR);
		}

		bindingsTable.completeHashTable(triplesBuilder.toString());
		List<String> replacedPredicatesTriples = this.replacePredicates(triplesBuilder.toString());
		binBuilder = new StringBuilder();
		pattern = PatBinCompressor.buildCompressedString(replacedPredicatesTriples);
		binding = binBuilder.toString().substring(0, binBuilder.length() - 1);

		return new PatBin(pattern, binding);
	}

	/**
	 * Extracts the triples from the file (replacing the prefixes using Jena's
	 * Model class), replaces the predicates obtained in the result by the
	 * bindings attributed in the map given as argument, and finally sort the
	 * result according to the subjects and predicates.
	 * 
	 * @param triples
	 *            The triples to be used.
	 * @return The list of triples with the replaced properties.
	 */
	private List<String> replacePredicates(String triples) {
		List<String> triplesWithReplacedPredicate = new ArrayList<String>();
		String[] splitedTriples = triples.split(SEPARATOR);
		CompressedTripleComparator comparator = new CompressedTripleComparator();

		// read triple by triple
		for (String triple : splitedTriples) {
			String predicate = triple.split(" ")[1];
			// replace the predicate
			triplesWithReplacedPredicate.add(triple.replace(predicate, bindingsTable.get(predicate).toString()));
		}

		// sorting the result
		Collections.sort(triplesWithReplacedPredicate, comparator);

		return triplesWithReplacedPredicate;
	}

	/**
	 * Constructs the compressed String using the List of triples with the
	 * replaced predicate.
	 * 
	 * @param triples
	 *            The List of triples with the replaced predicate.
	 * @return The compressed String.
	 */
	private static String buildCompressedString(List<String> triples) {
		Map<Integer, List<String>> predicateObjectCouples = new TreeMap<Integer, List<String>>();
		Set<String> subjectList = new TreeSet<String>();
		Set<String> objectList = new HashSet<String>();
		String previousSubject = triples.get(0).split(" ")[0];

		// initialization of the map and the subject list
		triplesMap = new TreeMap<String, Map<Integer, List<String>>>();
		for (String triple : triples) {
			// TODO we need to deal with multi-value predicates
			List<String> objects = new ArrayList<String>();
			String[] splitedTriple = triple.split(" ");

			subjectList.add(splitedTriple[0]);
			objectList.add(TripleManager.getObject(triple));
			if (splitedTriple[0].equals(previousSubject)) {
				objects.add(TripleManager.getObject(triple));
				predicateObjectCouples.put(Integer.parseInt(splitedTriple[1]), objects);
			} else {
				triplesMap.put(previousSubject, predicateObjectCouples);
				previousSubject = splitedTriple[0];
				predicateObjectCouples = new HashMap<Integer, List<String>>();
				objects.add(TripleManager.getObject(triple));
				predicateObjectCouples.put(Integer.parseInt(splitedTriple[1]), objects);
			}
		}
		triplesMap.put(previousSubject, predicateObjectCouples);

		subjectList.removeAll(objectList);
		Iterator<String> iterator = subjectList.iterator();
		if (subjectList.size() == 0) {
			System.out.println("All subjects are objects... not good.");
			System.exit(0);
		} else if (subjectList.size() == 1) {
			return recursiveBuildCompressedString(iterator.next());
		} else {
			StringBuilder builder = new StringBuilder();

			System.out.println("Several subjects are not objects, this is not a tree, but a forest.");
			while (iterator.hasNext()) {
				builder.append(recursiveBuildCompressedString(iterator.next()));
				if (iterator.hasNext()){
					builder.append(SEPARATOR);
					binBuilder.append(SEPARATOR);
				}
			}

			return builder.toString().trim();
		}
		return null;
	}

	/**
	 * Recursive function to build the compressed String.
	 * 
	 * @param currentKey
	 *            The current subject to extract the predicates' identifiers.
	 * @return
	 */
	private static String recursiveBuildCompressedString(String currentKey) {
		StringBuilder stringBuilder = new StringBuilder();
		Map<Integer, List<String>> predicateObjectCouples = triplesMap.get(currentKey);
		Set<Integer> keySet = new TreeSet<Integer>(predicateObjectCouples.keySet());

		for (int predicate : keySet) {
			stringBuilder.append(predicate);
			// TODO we need to deal with multi-value predicates
			String tripleObject = predicateObjectCouples.get(predicate).get(0);

			// build the binding, verifying if we have blank objects
			// (hexadecimal values)
			if (tripleObject.matches("[0-9a-fA-F]+"))
				binBuilder.append(" ;");
			else {
				// do not keep the " for the string litterals
				if (tripleObject.endsWith("\""))
					binBuilder.append(tripleObject.replace("\"", "") + ";");
				else {
					// TODO just remoe the uri... or it is impossible to process
					// (too long)
					/*if (tripleObject.startsWith("http"))
						binBuilder.append(" ;");
					else*/
						binBuilder.append(tripleObject + ";");
				}
			}

			// if the object is also a key, we need to create new leaves for the
			// tree
			if (triplesMap.get(tripleObject) != null) {
				stringBuilder.append(":(");
				stringBuilder.append(recursiveBuildCompressedString(tripleObject));
				stringBuilder.append(")");
			}
			stringBuilder.append(":");
		}

		stringBuilder.deleteCharAt(stringBuilder.length() - 1);

		return stringBuilder.toString();
	}

	/**
	 * Uncompress a PatBin compressed String.
	 * 
	 * @param compressedForm
	 *            The compressed String to be uncompressed.
	 * @param firstIdentifier
	 *            The first identifier to be used for the concepts.
	 * @return The triples
	 */
	public String uncompress(PatBin compressedForm, int firstIdentifier) {
		StringBuilder decompressedString = new StringBuilder();
		String result;

		tmpBindings = new ArrayList<String>();
		conceptIdentifier = firstIdentifier;
		for (String tree : compressedForm.getPattern().split(SEPARATOR)) {
			String compressedStringCopy = new String(tree);

			compressedStringCopy = compressedStringCopy.replaceAll("\\(", "(:");
			compressedStringCopy = compressedStringCopy.replaceAll("\\)", ":)");
			String[] splitedCompressedString = compressedStringCopy.split(":");

			recursiveUncompress(decompressedString, splitedCompressedString, conceptIdentifier, 0);
		}

		result = decompressedString.toString();
		if (compressedForm.getBinding() != null) {
			String[] splitedBindings = compressedForm.getBinding().split(";");
			for (int i = 0; i < splitedBindings.length; i++)
				if (!splitedBindings[i].equals(" "))
					result = result.replace(tmpBindings.get(i), splitedBindings[i] + " ");
		}

		return result.trim();
	}

	/**
	 * Recursive part of the decompression.
	 * 
	 * @param decompressedString
	 *            The uncompressed String (as a StringBuilder, because we need
	 *            it mutable).
	 * @param splitCompressedString
	 *            The compressed string, split in parenthesis and identifiers.
	 * @param currentConceptIdentifier
	 *            The current identifier of concept.
	 * @param currentPredicatePosition
	 *            The current position of the predicate in the split String.
	 */
	private void recursiveUncompress(StringBuilder decompressedString, String[] splitCompressedString,
			int currentConceptIdentifier, int currentPredicatePosition) {
		int incrementalIdentifier = currentConceptIdentifier + 1;
		int currentPosition;

		for (currentPosition = currentPredicatePosition; currentPosition < splitCompressedString.length; currentPosition++) {
			String currentValue = splitCompressedString[currentPosition];
			if (currentValue.equals(")")) {
				conceptIdentifier = incrementalIdentifier;
				predicatePosition = currentPosition;
				return;
			} else if (currentValue.equals("(")) {
				recursiveUncompress(decompressedString, splitCompressedString, incrementalIdentifier - 1,
						currentPosition + 1);
				currentPosition = predicatePosition;
				incrementalIdentifier = conceptIdentifier;
			} else {
				decompressedString.append("_:" + currentConceptIdentifier + " "
						+ bindingsTable.get(Integer.parseInt(currentValue)) + " _:" + incrementalIdentifier + " ");
				tmpBindings.add("_:" + incrementalIdentifier + " ");
				decompressedString.append(SEPARATOR);
				incrementalIdentifier++;
			}
		}
		conceptIdentifier = incrementalIdentifier;
		decompressedString.append(SEPARATOR);
	}

	/**
	 * Set the binding table.
	 * 
	 * @param table
	 *            The new binding table.
	 */
	public void setBindingTable(BindingsHashTable table) {
		bindingsTable = table;
	}

	/**
	 * Gives the next concept identifier to be used for decompression (useful
	 * when you want to combine uncompressions)
	 * 
	 * @return The last concept identifier.
	 */
	public int getNextConceptIdentifier() {
		return conceptIdentifier;
	}

	/**
	 * Builds the Bin from a series of triples; the triples are not necessarily
	 * sorted, but they have to get the objects for the Bin.
	 * 
	 * @param triples
	 *            The triples to extract the Bin from.
	 * @param table
	 *            The binding table to sort the triples.
	 * @return The Bin extracted from the triples.
	 */
	public static String makeBin(String triples, BindingsHashTable table) {
		StringBuilder result = new StringBuilder();
		List<String> splitedTriples = new ArrayList<String>();
		CompressedTripleComparator comparator = new CompressedTripleComparator();

		// split the triples, replace the predicate, sort the triples
		for (String triple : triples.split(SEPARATOR)) {
			String predicate = triple.split(" ")[1];
			splitedTriples.add(triple.replaceAll(predicate, table.get(predicate).toString()));
		}
		Collections.sort(splitedTriples, comparator);

		// build the Bin
		// remember to deal with string separators
		for (String triple : splitedTriples) {
			String object = triple.split(" ")[2];
			if (!object.startsWith("?"))
				if (!object.startsWith("\""))
					result.append(object);
				else
					result.append(triple.split("\"")[1]);
			else
				result.append(" ");
			result.append(";");
		}

		return result.substring(0, result.length() - 1);
	}

	public BindingsHashTable getTable() {
		return this.bindingsTable;
	}
}
