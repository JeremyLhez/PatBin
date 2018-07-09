package fr.waves_rsp.patbin.query.continuous;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jena.sparql.expr.aggregate.Aggregator;

import fr.waves_rsp.patbin.PatBin;
import fr.waves_rsp.patbin.PatBinManipulation;
import fr.waves_rsp.patbin.query.SPARQLQuery;

public class SPARQLOperatorsResult {

	/**
	 * Stores the values of the aggregates of a query for a given stream
	 * extract. The aggregates are not calculated or returned yet, just stored.
	 * 
	 * @param query
	 *            The query that has to store the aggregates.
	 * @param queryPatBin
	 *            The compressed form of the query.
	 * @param streamPatBin
	 *            The compressed form of the stream extract.
	 */
	public static void processAggregates(SPARQLQuery query, PatBin queryPatBin, PatBin streamPatBin) {
		// nothing to do if the query has no aggregate to process
		if (!query.hasAggregate())
			return;

		List<String> splitStreamBinding = PatBinManipulation.getUsefulStreamBinding(queryPatBin, streamPatBin);

		// add the values from the trimmed binding to the corresponding
		// aggregates
		Map<Integer, SPARQLAggregate> aggregates = query.getAggregatesValues();
		for (int pos : aggregates.keySet())
			aggregates.get(pos).addValue(splitStreamBinding.get(pos));
	}

	/**
	 * Gives the result of the aggregates of a query, with the values currently
	 * stored.
	 * 
	 * @param query
	 *            The query to be processed.
	 * @return A map with the AS variable of each aggregate and its result.
	 */
	public static Map<String, String> getAggregatesResult(SPARQLQuery query) {
		Map<String, String> aggregateValues = new HashMap<String, String>();

		if (!query.hasAggregate())
			return aggregateValues;

		int i = 0;
		List<String> asVariables = new ArrayList<String>(query.getAggregate().values());
		for (SPARQLAggregate agg : query.getAggregatesValues().values()) {
			aggregateValues.put(asVariables.get(i), agg.getCurrentResult() + "");
			i++;
		}

		return aggregateValues;
	}

	/**
	 * Checks if the all the aggregates of a query are valid. The query must
	 * have been executed at least one time, so that there are aggregates
	 * already computed for the result.
	 * 
	 * @param query
	 *            The query to be used.
	 * @return true if the aggregates are valid, false otherwise.
	 */
	public static boolean aggregatesAreValid(SPARQLQuery query) {
		// TODO deals only with float types at the moment
		if (!query.hasHaving())
			return true;

		List<Float> result = new ArrayList<Float>();
		for (SPARQLAggregate agg : query.getAggregatesValues().values())
			result.add(agg.getCurrentResult());

		List<String> aggregates = new ArrayList<String>();
		for (Aggregator a : query.getAggregate().keySet())
			aggregates.add(a.toString());
		for (String having : query.getHaving()) {
			String[] split = having.split(" ");
			float value = Float.parseFloat(split[2]);
			int index = aggregates.indexOf(split[0]);

			switch (split[1]) {
			case "<":
				if (!(result.get(index) < value))
					return false;
				break;
			case ">":
				if (!(result.get(index) > value))
					return false;
				break;
			case "<=":
				if (!(result.get(index) <= value))
					return false;
				break;
			case ">=":
				if (!(result.get(index) >= value))
					return false;
				break;
			case "==":
				if (result.get(index) != value)
					return false;
				break;
			case "!=":
				if (result.get(index) == value)
					return false;
				break;
			default:
				throw new IllegalArgumentException();
			}
		}

		return true;
	}

}
