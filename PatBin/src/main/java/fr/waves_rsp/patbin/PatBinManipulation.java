package fr.waves_rsp.patbin;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.sparql.engine.http.QueryEngineHTTP;

import fr.waves_rsp.patbin.compressor.BindingsHashTable;
import fr.waves_rsp.patbin.compressor.PatBinCompressor;

public class PatBinManipulation {
	private static final String SEPARATOR = System.getProperty("line.separator");
	private static List<String> selectValues;

	/**
	 * Compares the pattern from a stream, and the one from a query, to give the
	 * common triples.
	 * 
	 * @param streamPattern
	 *            The stream pattern (in a compressed form).
	 * @param queryPattern
	 *            The query pattern (in a compressed form).
	 * @return The common pattern (in a compressed form).
	 */
	public static String jointPattern(String streamPattern, String queryPattern) {
		StringBuilder outputPattern = new StringBuilder();
		String output;

		String streamElement, queryElement;

		// spliting the stream pattern
		String streamPatternCopy = streamPattern.replaceAll("\\(", "(:");
		streamPatternCopy = streamPatternCopy.replaceAll("\\)", ":)");
		List<String> streamPatternSplit = new ArrayList<String>(Arrays.asList(streamPatternCopy.split(":")));
		// spliting the query pattern
		String queryPatternCopy = queryPattern.replaceAll("\\(", "(:");
		queryPatternCopy = queryPatternCopy.replaceAll("\\)", ":)");
		List<String> queryPatternSplit = new ArrayList<String>(Arrays.asList(queryPatternCopy.split(":")));

		while (!streamPatternSplit.isEmpty() && !queryPatternSplit.isEmpty()) {
			streamElement = streamPatternSplit.get(0);
			queryElement = queryPatternSplit.get(0);
			if (streamElement.equals("(")) {
				// stream has an opening parenthesis
				if (queryElement.equals("(")) {
					// both opening parenthesis, OK
					outputPattern.append("(");
					streamPatternSplit.remove(0);
					queryPatternSplit.remove(0);
				} else {
					// query has something else: the branch in the stream
					// pattern must be ignored
					removeBranchFromPattern(streamPatternSplit);
				}
			} else if (streamElement.equals(")")) {
				// stream has a closing parenthesis
				if (queryElement.equals(")")) {
					// both closing parenthesis: OK
					outputPattern.append("):");
					streamPatternSplit.remove(0);
					queryPatternSplit.remove(0);
				} else if (queryElement.equals("(")) {
					// query has a new branch: it must be ignored
					removeBranchFromPattern(queryPatternSplit);
				} else {
					// query has a binding: it must be ignored
					queryPatternSplit.remove(0);
				}
			} else {
				if (queryElement.equals(streamElement)) {
					// both the same binding: OK
					outputPattern.append(streamElement + ":");
					streamPatternSplit.remove(0);
					queryPatternSplit.remove(0);
				} else if (queryElement.equals("(")) {
					// query has a new branch: it must be ignored
					removeBranchFromPattern(queryPatternSplit);
				} else if (queryElement.equals(")")) {
					// query has finished the current branch: ignore the binding
					// of stream
					streamPatternSplit.remove(0);
				} else {
					// both have bindings, but different: remove the smallest
					if (Integer.parseInt(streamElement) < Integer.parseInt(queryElement))
						streamPatternSplit.remove(0);
					else
						queryPatternSplit.remove(0);
				}
			}
		}

		// cleaning the output
		output = outputPattern.substring(0, outputPattern.length() - 1);
		output = output.replace(":)", ")");
		output = cleanJointPattern(output);

		return output;
	}

	/**
	 * Subtraction of the query pattern and the common pattern (between the
	 * query and the stream), to get what needs to be materialized (in a
	 * compressed form).
	 * 
	 * @param queryPattern
	 *            The query pattern (in a compressed form).
	 * @param jointPattern
	 *            The common pattern (in a compressed form).
	 * @return The triples that need to be materialized (in a compressed form).
	 */
	public static String minusPattern(String queryPattern, String jointPattern) {
		StringBuilder outputPattern = new StringBuilder();

		// spliting the query pattern
		String queryPatternCopy = queryPattern.replaceAll("\\(", "(:");
		queryPatternCopy = queryPatternCopy.replaceAll("\\)", ":)");
		List<String> queryPatternSplit = new ArrayList<String>(Arrays.asList(queryPatternCopy.split(":")));
		// spliting the common pattern
		String jointPatternCopy = jointPattern.replaceAll("\\(", "(:");
		jointPatternCopy = jointPatternCopy.replaceAll("\\)", ":)");
		List<String> jointPatternSplit = new ArrayList<String>(Arrays.asList(jointPatternCopy.split(":")));

		String jointElement, queryElement;

		while (!jointPatternSplit.isEmpty()) {
			jointElement = jointPatternSplit.get(0);
			queryElement = queryPatternSplit.get(0);

			if (queryElement.equals("(")) {
				// query has a "("
				if (jointElement.equals("(")) {
					// both have a "(", we add it in case there are some deeper
					// branches to complete
					outputPattern.append("(");
					jointPatternSplit.remove(0);
					queryPatternSplit.remove(0);
				} else {
					// if joint has a pattern id, we need to complete the
					// current query branch; same for ")"
					outputPattern.append(removeBranchFromPattern(queryPatternSplit));
				}
			} else if (queryElement.equals(")")) {
				queryPatternSplit.remove(0);
				// query has a closing parenthesis
				if (jointElement.equals(")")) {
					// both have a ")", we add it in case there are some deeper
					// branches to complete
					outputPattern.append("):");
					jointPatternSplit.remove(0);
				}
				// no other possibility
			} else {
				queryPatternSplit.remove(0);
				// query has a pattern id
				if (!jointElement.equals(queryElement))
					// both are different (different id, parenthesis for joint):
					// we add the pattern id of the query
					outputPattern.append(queryElement + ":");
				else {
					// otherwise, they are equal: add the id if the next element
					// is a "("
					jointPatternSplit.remove(0);
					if (queryPatternSplit.get(0).equals("("))
						outputPattern.append(queryElement + ":");
				}
			}
		}

		// if the joint is empty, the query may not be
		while (!queryPatternSplit.isEmpty()) {
			queryElement = queryPatternSplit.remove(0);
			outputPattern.append(queryElement);

			// if there is a next element and if we don't have an opening
			// parenthesis, and the next element is not a closing parenthesis,
			// we add ":"
			if (!queryPatternSplit.isEmpty())
				if ((!queryPatternSplit.get(0).equals(")")) && (!queryElement.equals("(")))
					outputPattern.append(":");
		}

		return PatBinManipulation.cleanGeneralPattern(outputPattern.toString());
	}

	/**
	 * Builds the Bin that need to be materialized in the stream, using a non-compressed stream.
	 * 
	 * @param stream
	 *            The specific stream.
	 * @param bindingTable
	 *            The table used for all the patterns.
	 * @param matPattern
	 *            The compressed pattern to be materialized.
	 * @param staticProperties
	 *            The table of static properties in the knowledge base.
	 * @param sparqlEndpoint
	 *            The endpoint to query the static knowledge base.
	 * @return The materialized Bin.
	 */
	public static String getMaterializedBin(InputStream stream, BindingsHashTable bindingTable, String matPattern,
			Map<Integer, Integer> staticProperties, String sparqlEndpoint) {
		StringBuilder triplesBuilder = new StringBuilder();
		Model streamModel = ModelFactory.createDefaultModel();
		String staticStreamTriples;
		StmtIterator stmtIterator;
		Triple triple;

		streamModel.read(stream, null, "TTL");
		stmtIterator = streamModel.listStatements();
		// STEP1: extract the static triples from the stream
		List<Triple> validTriples = new ArrayList<Triple>();
		List<Node> validSubjects = new ArrayList<Node>();
		while (stmtIterator.hasNext()) {
			triple = stmtIterator.nextStatement().asTriple();
			// get the predicate, its binding, see if it is static
			if (staticProperties.get(bindingTable.get(triple.getPredicate().toString())) != null) {
				validTriples.add(triple);
				validSubjects.add(triple.getSubject());
			}
		}
		// delete the useless triples (blank node as object, which is not a
		// subject)
		for (Triple t : validTriples) {
			if (t.getObject().isBlank())
				if (!validSubjects.contains(t.getObject()))
					continue;
			triplesBuilder.append(t.toString() + SEPARATOR);
		}
		staticStreamTriples = triplesBuilder.toString();

		// STEP2: uncompress what needs to be materialized
		String materializedTriples;
		PatBinCompressor compressor = new PatBinCompressor();
		compressor.setBindingTable(bindingTable);
		PatBin matPatBin = new PatBin(matPattern, null);
		materializedTriples = compressor.uncompress(matPatBin, 0);

		// STEP3: build the correct WHERE content of the final query (adapting
		// blank nodes)
		String[] splitL1, splitL2 = new String[3];
		String[] splitStaticStreamTriples = staticStreamTriples.split(SEPARATOR);
		String[] splitMaterializedTriples = materializedTriples.split(SEPARATOR);
		boolean adapted = false;
		for (String line : splitMaterializedTriples) {
			splitL1 = line.split(" ");

			for (String l : splitStaticStreamTriples) {
				String[] tmp = l.split(" ");
				if (tmp[2].startsWith("\"")) {
					splitL2[0] = tmp[0];
					splitL2[1] = tmp[1];
					splitL2[2] = l.split("\"")[1];
				} else
					splitL2 = tmp;

				// predicates identical: we will use the same subject & objects
				// in both cases; also, delete the triple, or it will be a
				// duplicate
				if ((splitL1[1]).equals(splitL2[1])) {
					staticStreamTriples = staticStreamTriples.replaceAll(splitL2[0], splitL1[0]);
					staticStreamTriples = staticStreamTriples.replaceAll(splitL2[2], splitL1[2]);
					staticStreamTriples = staticStreamTriples.replace(line + SEPARATOR, "");
					adapted = true;
				}
			}
		}
		// if there is no link between the materialization & the static triples,
		// we need to join the subjects
		if (!adapted) {
			String id = splitMaterializedTriples[0].split(" ")[0];
			staticStreamTriples = staticStreamTriples.replaceAll(splitStaticStreamTriples[0].split(" ")[0], id);
		}
		triplesBuilder = new StringBuilder();
		triplesBuilder.append(staticStreamTriples.replaceAll(" @", " "));
		triplesBuilder.append(materializedTriples);

		// STEP4: build and execute the query to fetch the values to be
		// materialized
		String sparqlQuery = buildMaterializationQuery(triplesBuilder.toString());

		Query query = QueryFactory.create(sparqlQuery);
		QueryEngineHTTP queryEngine = QueryExecutionFactory.createServiceRequest(sparqlEndpoint, query);
		ResultSet result = queryEngine.execSelect();

		// // STEP5: build the final Bin
		materializedTriples = materializedTriples.replaceAll("_:", "?x");
		if (result.hasNext()) {
			QuerySolution solution = result.nextSolution();
			for (String variable : selectValues)
				materializedTriples = materializedTriples.replace(variable, solution.getLiteral(variable).toString());
		}

		// need to close the engine for several queries
		queryEngine.close();

		return PatBinCompressor.makeBin(materializedTriples, bindingTable);
	}

	/**
	 * Generate a new PatBin for a stream, with the materialized values.
	 * 
	 * @param streamPattern
	 *            The current pattern of the stream.
	 * @param streamBinding
	 *            The current binding of the stream.
	 * @param matPattern
	 *            the materialization pattern.
	 * @param matBinding
	 *            The materialization binding.
	 * @param table
	 *            The binding table used for the compression/decompression.
	 * @return A map with ONE couple, with the new pattern as the key, and the
	 *         new binding as its value.
	 */
	public static PatBin generateNewStreamBin(PatBin streamPatBin, PatBin matPatBin, BindingsHashTable table) {
		Map<Integer, String> propValueMap = new HashMap<Integer, String>();
		String[] splitStreamBinding, splitMatBinding, splitStreamPattern, splitMatPattern;

		// create a map associating each predicate to its value
		splitStreamPattern = streamPatBin.getPattern().replaceAll("\\(", "").replaceAll("\\)", "").split(":");
		splitMatPattern = matPatBin.getPattern().replaceAll("\\(", "").replaceAll("\\)", "").split(":");
		splitStreamBinding = streamPatBin.getBinding().split(";", -1);
		splitMatBinding = matPatBin.getBinding().split(";", -1);

		for (int i = 0; i < splitStreamPattern.length; i++)
			propValueMap.put(Integer.parseInt(splitStreamPattern[i]), splitStreamBinding[i]);
		for (int i = 0; i < splitMatPattern.length; i++)
			propValueMap.put(Integer.parseInt(splitMatPattern[i]), splitMatBinding[i]);

		// uncompress the stream pattern
		PatBinCompressor compressor = new PatBinCompressor();
		compressor.setBindingTable(table);
		PatBin spb = new PatBin(streamPatBin.getPattern(), null);
		String uncompressedStream = compressor.uncompress(spb, 0);

		// uncompress the materialization pattern
		PatBin mpb = new PatBin(matPatBin.getPattern(), null);
		String uncompressedMat = compressor.uncompress(mpb, compressor.getNextConceptIdentifier());

		// build the new triples for the stream
		List<String> splitedUncompressedStream = new ArrayList<String>(
				Arrays.asList(uncompressedStream.split(SEPARATOR)));
		List<String> splitedUncompressedMat = new ArrayList<String>(Arrays.asList(uncompressedMat.split(SEPARATOR)));
		List<String> result = new ArrayList<String>(splitedUncompressedStream);
		// remove the triples to be materialized one by one
		boolean adapted = false;
		int numberAdapted = 0;
		while (splitedUncompressedMat.size() != 0) {
			String matTriple = splitedUncompressedMat.remove(0);

			for (String triple : splitedUncompressedStream) {
				String[] matSplit = matTriple.split(" ");
				String[] resSplit = triple.split(" ");
				// if the triple to be materialized has its predicate already in
				// the result, we adjust the identifiers of the concepts
				if (resSplit[1].equals(matSplit[1])) {
					for (String s : splitedUncompressedMat) {
						String tmp;
						tmp = s.replaceFirst(matSplit[0], resSplit[0]);
						tmp = tmp.replaceFirst(matSplit[2], resSplit[2]);
						splitedUncompressedMat.set(splitedUncompressedMat.indexOf(s), tmp);
					}
					// we are done, we found it
					adapted = true;
					numberAdapted++;
					break;
				}
			}
			// if not, then it needs to be added
			if (!adapted)
				result.add(matTriple);
			adapted = false;
		}

		// clean the new triples for the stream
		StringBuilder newPatternBuilder = new StringBuilder();
		for (String s : result) {
			String[] splitedTriple = s.split(" ");
			newPatternBuilder.append(splitedTriple[0] + " <" + splitedTriple[1] + "> " + splitedTriple[2] + " .");
			newPatternBuilder.append(SEPARATOR);
		}
		// if there are no predicates in common between the materialization and
		// the stream, we still have to adapt the subjects.
		if (numberAdapted == 0) {
			String id = uncompressedStream.split(SEPARATOR)[0].split(" ")[0];
			newPatternBuilder = new StringBuilder(
					newPatternBuilder.toString().replaceAll(uncompressedMat.split(" ")[0], id));
		}
		// build the new pattern for the stream
		PatBin newpatBin = compressor
				.compressFile(new ByteArrayInputStream(newPatternBuilder.toString().trim().getBytes()), "TTL");

		// build the Bin
		StringBuilder binBuilder = new StringBuilder();
		for (String i : newpatBin.getPattern().replaceAll("\\(", "").replaceAll("\\)", "").split(":"))
			binBuilder.append(propValueMap.get(Integer.parseInt(i)) + ";");
		String newStreamBinding = binBuilder.substring(0, binBuilder.length() - 1);

		return new PatBin(newpatBin.getPattern(), newStreamBinding);
	}

	/**
	 * Removes a branch from a split pattern, and returns it in a correct
	 * compressed form.
	 * 
	 * @param splitPattern
	 *            The split pattern to be used.
	 * @return The branch removed.
	 */
	private static String removeBranchFromPattern(List<String> splitPattern) {
		StringBuilder branch = new StringBuilder();
		int parenthesis = 1;

		branch.append(splitPattern.remove(0));

		while (parenthesis > 0) {
			String element = splitPattern.remove(0);

			if (element.equals("(")) {
				parenthesis += 1;
				branch.append(element);
			} else if (element.equals(")")) {
				parenthesis -= 1;
				if (branch.toString().endsWith(":"))
					branch.replace(branch.length() - 1, branch.length(), ")");
				else if (parenthesis > 0)
					branch.append(")");
			} else
				branch.append(element + ":");
		}

		return branch.toString();
	}

	/**
	 * Cleans a pattern form: removes the unnecessary parenthesis. WARNING: does
	 * NOT remove the bindings associated to the empty parenthesis (not adapted
	 * for cleaning minusPattern if needed)
	 * 
	 * @param pattern
	 *            The pattern to be cleaned
	 * @return The cleaned form of the pattern.
	 */
	private static String cleanJointPattern(String pattern) {
		String result = pattern;

		while (result.contains(":()"))
			result = result.replaceAll(":\\(\\)", "");

		return result;
	}

	/**
	 * Clean a pattern in a general use case: removes the empty parenthesis and
	 * their associated pattern id, and the misplaced ":".
	 * 
	 * @param pattern
	 *            The pattern to be cleansed.
	 * @return The clean pattern.
	 */
	private static String cleanGeneralPattern(String pattern) {
		String result = pattern;
		boolean change = true;

		while (change == true) {
			change = false;
			if (result.toString().contains(":)")) {
				result = result.replaceAll(":\\)", ")");
				change = true;
			}
			if (result.toString().contains("(:")) {
				result = result.replaceAll("\\(:", "(");
				change = true;
			}
			if (result.toString().contains("()")) {
				result = result.replaceAll("[0-9]+:\\(\\)", "");
				change = true;
			}
			if (result.toString().contains("::")) {
				result = result.replaceAll("::", ":");
				change = true;
			}
			if (result.toString().startsWith(":")) {
				result = result.toString().replaceFirst(":", "");
				change = true;
			}
			if (result.toString().endsWith(":")) {
				result = result.substring(0, result.length() - 1);
				change = true;
			}
		}

		return result;
	}

	/**
	 * Builds the query that will fetch the values that need to be materialized
	 * for a stream. Basically, uses the triples in the WHERE clause to identify
	 * the values that need to be put in the SELECT, then builds the complete
	 * query.
	 * 
	 * @param whereTriples
	 *            The triples that are in the WHERE clause.
	 * @return The query generated.
	 */
	private static String buildMaterializationQuery(String whereTriples) {
		StringBuilder query = new StringBuilder();
		List<String> subjects = new ArrayList<String>(), objects = new ArrayList<String>();
		selectValues = new ArrayList<String>();
		String[] splitedTriple, splitedTriples;
		String triples;

		query.append("SELECT ");
		// add the blank nodes we want
		triples = whereTriples.replaceAll("_:", "?x");
		splitedTriples = triples.split(SEPARATOR);
		for (String line : splitedTriples) {
			splitedTriple = line.split(" ");
			if (splitedTriple[0].startsWith("?x"))
				subjects.add(splitedTriple[0]);
			if (splitedTriple[2].startsWith("?x"))
				objects.add(splitedTriple[2]);
			triples = triples.replaceAll(" " + splitedTriple[1] + " ", " <" + splitedTriple[1] + "> ");
		}
		for (String object : objects) {
			if (!subjects.contains(object)) {
				query.append(object + " ");
				selectValues.add(object);
			}
		}

		query.append("WHERE {" + SEPARATOR);
		// add the triples for the where
		triples = triples.replaceAll(SEPARATOR, " ." + SEPARATOR);
		query.append(triples + SEPARATOR + "}");

		return query.toString();
	}

	/**
	 * Creates the list of bindings from the stream that are useful for the
	 * query: no more, no less. One can see it as the binding of the query
	 * extracted from the stream (thus with the required information).
	 * 
	 * @param queryPattern
	 *            The pattern of the query.
	 * @param streamPattern
	 *            The pattern of the stream.
	 * @return The list of bindings in the stream that correspond exactly to the
	 *         query (additional information not present in the query has been
	 *         removed).
	 */
	public static List<String> getUsefulStreamBinding(PatBin queryPatBin, PatBin streamPatBin) {
		// split patterns/bindings in lists of identifiers/values
		List<String> splitQueryPattern = new ArrayList<String>(
				Arrays.asList(queryPatBin.getPattern().replaceAll("\\(", "").replaceAll("\\)", "").split(":")));
		List<String> splitStreamPattern = new ArrayList<String>(
				Arrays.asList(streamPatBin.getPattern().replaceAll("\\(", "").replaceAll("\\)", "").split(":")));
		List<String> splitStreamBinding = new ArrayList<String>(
				Arrays.asList(streamPatBin.getBinding().split(";", -1)));
		List<Integer> removedIdentifiersIndexes = new ArrayList<Integer>();

		// remove the identifiers from the stream pattern that are not required
		// in the query
		int index = 0;
		while (index < splitQueryPattern.size()) {
			// make the beginning of the stream correspond to the query
			if (!splitQueryPattern.get(index).equals(splitStreamPattern.get(index))) {
				splitStreamPattern.remove(index);
				removedIdentifiersIndexes.add(index + removedIdentifiersIndexes.size());
			} else
				index++;
		}
		while (splitQueryPattern.size() != splitStreamPattern.size()) {
			// if not enough, clean the end too
			splitStreamPattern.remove(index);
			removedIdentifiersIndexes.add(index + removedIdentifiersIndexes.size());
		}

		// remove the values from the stream binding that are not required in
		// the query
		int k = removedIdentifiersIndexes.size() - 1;
		while (k >= 0) {
			// reverse to keep the good identifiers, otherwise it does not
			// remove the good ones (since the position of the last ones
			// changes)
			splitStreamBinding.remove(removedIdentifiersIndexes.get(k).intValue());
			k--;
		}

		return splitStreamBinding;
	}

}
