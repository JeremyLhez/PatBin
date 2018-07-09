package fr.waves_rsp.patbin.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import fr.waves_rsp.patbin.PatBinManipulation;

public class CompressedQueryResolver {
	private String queryPattern, queryBinding;
	private String streamPattern;
	private List<Integer> indexesToRemove;
	private Map<String, AggregatesStorage> aggregates;

	/**
	 * Default constructor
	 * 
	 * @param qp
	 *            Query pattern.
	 * @param qb
	 *            Query binding.
	 * @param sp
	 *            Stream pattern.
	 */
	public CompressedQueryResolver(String qp, String qb, String sp) {
		this.queryPattern = qp;
		this.queryBinding = qb;
		this.streamPattern = sp;
		this.indexesToRemove = null;
		this.aggregates = new HashMap<String, AggregatesStorage>();
	}

	/**
	 * Initialization: gets the different steps to be done to have a query
	 * PatBin size corresponding to a streamPatBin size (if it is not possible,
	 * the query is invalid).
	 */
	public void initialize() {
		String supPattern = PatBinManipulation.minusPattern(streamPattern,
				PatBinManipulation.jointPattern(streamPattern, queryPattern));

		// TODO here should be the materialization step

		// initialize the parts to remove
		if (supPattern.length() == 0)
			indexesToRemove = new ArrayList<Integer>();
		else
			indexesToRemove = getUselessIndexes();
	}

	/**
	 * Execute the query on a binding. The query must be initialized before.
	 * 
	 * @param streamBinding
	 *            The binding on which the query shall be executed.
	 * @return The valus for the variables.
	 */
	public Map<String, String> execute(String streamBinding) {
		// normalize the bindings, using the structures established at the
		// initialization
		// TODO there should be a materialization part too
		int i = indexesToRemove.size() - 1;
		List<String> splitStreamBinding = new ArrayList<String>(Arrays.asList(streamBinding.split(";")));
		while (i >= 0) {
			int pos = indexesToRemove.get(i).intValue();
			splitStreamBinding.remove(pos);
			i--;
		}

		// final check to see if the query is valid for the stream
		String[] splitQueryBinding = queryBinding.split(";");
		if (splitStreamBinding.size() != splitQueryBinding.length)
			return null;

		// initialize the list of positions of the variables
		List<Integer> variablesPosition = new ArrayList<Integer>();
		for (int k = 0; k < splitQueryBinding.length; k++) {
			if (splitQueryBinding[k].startsWith("?"))
				variablesPosition.add(k);
		}

		String newBin = String.join(";", splitStreamBinding);
		Map<String, String> result = queryOnStream(newBin, variablesPosition);

		if (result != null)
			processAggregates(newBin, variablesPosition);

		return result;
	}

	/**
	 * Compares the patterns of the query and the stream to get the position of
	 * the values irrelevant for the query. These values can thus be removed to
	 * get common pattern and binding for both, but this should be done backward
	 * for the identifiers to match.
	 * 
	 * @return The list of the irrelevant indexes of this stream for this query.
	 */
	private List<Integer> getUselessIndexes() {
		// split patterns/bindings in lists of identifiers/values
		List<String> splitQueryPattern = new ArrayList<String>(
				Arrays.asList(queryPattern.replaceAll("\\(", "").replaceAll("\\)", "").split(":")));
		List<String> splitStreamPattern = new ArrayList<String>(
				Arrays.asList(streamPattern.replaceAll("\\(", "").replaceAll("\\)", "").split(":")));
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

		// if not enough, clean the end too
		while (splitQueryPattern.size() != splitStreamPattern.size()) {
			splitStreamPattern.remove(index);
			removedIdentifiersIndexes.add(index + removedIdentifiersIndexes.size());
		}

		return removedIdentifiersIndexes;
	}

	/**
	 * Apply the query on the stream, and returns the values of the different
	 * variables. Does NOT make the matching operations of the query's PatBin
	 * with the stream's, this step must be made before (initialization)
	 * 
	 * @param streamBinding
	 *            A binding coming from the stream.
	 * @param valuesIndexes
	 *            The indexes of the variables.
	 * @return A map of variable-value, or null if the query has no result on
	 *         the given binding.
	 */
	private Map<String, String> queryOnStream(String streamBinding, List<Integer> valuesIndexes) {
		Map<String, String> variablesValues = new HashMap<String, String>();
		String[] splitStreamBinding = streamBinding.split(";");
		String[] splitQueryBinding = queryBinding.split(";");

		// verify each element of the binding
		for (int i = 0; i < splitQueryBinding.length; i++) {
			// if the element is a variable
			if (valuesIndexes.contains(i)) {
				// check if it has a filter, and if it is valid.
				if (applyFilters(splitQueryBinding[i], splitStreamBinding[i]))
					variablesValues.put(splitQueryBinding[i], splitStreamBinding[i]);
				else
					return null;
			} else {
				// the element is not a variable; we check for compatibility
				// with the query (filters ?)
				if (!validateStandardBinding(splitQueryBinding[i], splitStreamBinding[i])) {
					return null;
				}
			}
		}

		return variablesValues;
	}

	/**
	 * Apply the filter and/or aggregate (and having, if present) to a variable,
	 * verifying if the result is valid.
	 * 
	 * @param variable
	 *            The complete variable, with the details after, as represented
	 *            in the binding.
	 * @param value
	 *            The value found in the stream.
	 * @return true if the value found is valid, false otherwise.
	 */
	private static boolean applyFilters(String variable, String value) {
		int pos;

		if ((pos = variable.indexOf('<')) != -1) {
			float nb = Float.parseFloat(value);
			// <=
			if (variable.charAt(pos + 1) == '=') {
				if (nb <= Float.parseFloat(variable.substring(pos + 2)))
					return true;
				else
					return false;
			}
			// <
			else {
				if (nb < Float.parseFloat(variable.substring(pos + 1)))
					return true;
				else
					return false;
			}
		} else if ((pos = variable.indexOf('>')) != -1) {
			float nb = Float.parseFloat(value);
			// >=
			if (variable.charAt(pos + 1) == '=') {
				if (nb >= Float.parseFloat(variable.substring(pos + 2)))
					return true;
				else
					return false;
			}
			// >
			else {
				if (nb > Float.parseFloat(variable.substring(pos + 1)))
					return true;
				else
					return false;
			}
		} else if ((pos = variable.indexOf('=')) != -1) {
			if (variable.charAt(pos + 1) == '"') {
				if (value.equals(variable.substring(pos + 2, variable.length() - 1)))
					return true;
				else
					return false;
			}
			float nb = Float.parseFloat(value);
			// =
			if (nb == Float.parseFloat(variable.substring(pos + 1)))
				return true;
			else
				return false;
		} else
			return true;
	}

	/**
	 * Verifies if a value from a stream binding is valid according to the value
	 * at the corresponding place in the query. Does not take into account
	 * variables (both standard and aggregate ones).
	 * 
	 * @param binQuery
	 *            The value from the binding of the query.
	 * @param binStream
	 *            The value from the binding of the stream.
	 * @return true if the value of the stream is valid, false otherwise.
	 */
	private static boolean validateStandardBinding(String binQuery, String binStream) {
		// if the query has no requirement at this position, it's ok
		if (binQuery.equals(" "))
			return true;

		// if the bindings are equal, it's ok
		if (binQuery.equals(binStream))
			return true;

		// if it is a filter
		if (binQuery.startsWith("<")) {
			if (binQuery.charAt(1) == '=') {
				if (Float.parseFloat(binStream) <= Float.parseFloat(binQuery.substring(2)))
					return true;
			} else {
				if (Float.parseFloat(binStream) < Float.parseFloat(binQuery.substring(1)))
					return true;
			}
		} else if (binQuery.startsWith(">")) {
			if (binQuery.charAt(1) == '=') {
				if (Float.parseFloat(binStream) >= Float.parseFloat(binQuery.substring(2)))
					return true;
			} else {
				if (Float.parseFloat(binStream) > Float.parseFloat(binQuery.substring(1)))
					return true;
			}
		} else if (binQuery.startsWith("="))
			if (Float.parseFloat(binStream) == Float.parseFloat(binQuery.substring(1)))
				return true;

		// in any other case, it's not ok
		return false;
	}

	/**
	 * Processes the aggregates. There is a map with the variables values as
	 * key, and the class storing the aggregates as value. If a new key is met,
	 * a new couple is created and added to the map (this is the principle of
	 * the group by). Otherwise we add the value to the existing structure.
	 * 
	 * @param newBin
	 *            The new binding
	 * @param valuesIndexes
	 */
	private void processAggregates(String newBin, List<Integer> valuesIndexes) {
		String[] splitNewStreamBin = newBin.split(";");
		String[] splitQueryBin = queryBinding.split(";");
		StringBuilder key = new StringBuilder(splitNewStreamBin[valuesIndexes.get(0)]);

		// build the key (group by)
		for (int i = 1; i < valuesIndexes.size(); i++) {
			String elem = splitNewStreamBin[valuesIndexes.get(i)];
			if (!elem.substring(1).contains("?"))
				key.append(";" + elem);
		}

		// initialize the aggregates
		for (int i : valuesIndexes) {
			String tmp = splitQueryBin[i].substring(1);
			// the aggregates
			if (tmp.contains("?")) {
				if (aggregates.get(key) != null) {
					this.aggregates.get(key).addAggregateValue(splitNewStreamBin[i]);
				} else {
					AggregatesStorage a;
					String end = tmp.substring(tmp.indexOf("?") + 1);
					// the having (or not)
					if (end.startsWith("COUNT")) {
						if (end.length() > 5)
							a = new AggregatesStorage(end.substring(0, 5), end.substring(5));
						else
							a = new AggregatesStorage(end);
					} else {
						if (end.length() > 3)
							a = new AggregatesStorage(end.substring(0, 3), end.substring(3));
						else
							a = new AggregatesStorage(end);
					}
					a.addAggregateValue(splitNewStreamBin[i]);
					aggregates.put(key.toString(), a);
				}
			}

		}
	}

	/**
	 * Process the result of the aggregates.
	 * 
	 * @return The result of the aggregates (values), associated to the
	 *         variables (keys, group by). If the aggregate is invalid (having),
	 *         the result is null.
	 */
	public Map<String, Float> aggregatesResult() {
		Map<String, Float> result = new HashMap<String, Float>();

		for (String key : aggregates.keySet()) {
			AggregatesStorage agg = aggregates.get(key);
			if (!agg.process())
				result.put(key, null);
			else
				result.put(key, agg.getResult());
		}

		return result;
	}
}
