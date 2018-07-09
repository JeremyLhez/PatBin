package fr.waves_rsp.patbin.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.SortCondition;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprAggregator;
import org.apache.jena.sparql.expr.ExprFunction;
import org.apache.jena.sparql.expr.aggregate.Aggregator;

import fr.waves_rsp.patbin.compressor.BindingsHashTable;
import fr.waves_rsp.patbin.query.continuous.SPARQLAggregate;
import fr.waves_rsp.patbin.util.CompressedTripleComparator;

public class SPARQLQueryManager {

	/**
	 * Converts a query into a class where the different elements (operators,
	 * variables, ...) are separated and easier to use.
	 * 
	 * @param query
	 *            The query to be used.
	 * @return The new representation of the query.
	 */
	public static SPARQLQuery extractQueryElements(String query) {
		Map<String, String> variables = new HashMap<String, String>();
		SPARQLQuery result;
		List<ExprAggregator> aggregators;
		List<SPARQLAggregate> aggregatorsValues = new ArrayList<SPARQLAggregate>();
		String where, triples;
		Query jenaQuery;
		int aggregatePos = 0, nodeID = 1;

		jenaQuery = QueryFactory.create(query);
		aggregators = jenaQuery.getAggregators();

		// extract the WHERE
		// WARNING: doesn't use jena here
		where = query.substring(query.indexOf("{") + 1, query.lastIndexOf("}")).trim();
		triples = where;
		for (String line : where.split(System.lineSeparator())) {
			for (String part : line.split(" "))
				if (part.startsWith("?") && !variables.keySet().contains(part)) {
					variables.put(part, "_:" + nodeID);
					triples = triples.replace(part + " ", "_:" + nodeID + " ");
					nodeID++;
				}
		}

		result = new SPARQLQuery(query, triples, variables);

		// extract the SELECT variables
		for (Var v : jenaQuery.getProjectVars()) {
			String vString = v.toString();
			if (variables.keySet().contains(vString))
				result.addSelectVariable(vString, variables.get(vString));
			else {
				// deal with the aggregates: store them in the query &
				// initialize the list of value to compute their result
				Aggregator agg = aggregators.get(aggregatePos).getAggregator();
				// add the aggregator
				result.addAggregate(vString, agg);
				// initialize its list of future values
				aggregatorsValues.add(new SPARQLAggregate(agg.toString().split("\\(")[0]));
				aggregatePos++;
			}
		}

		// extract the GROUP BY
		VarExprList groupBy = jenaQuery.getGroupBy();
		if (groupBy != null)
			if (!groupBy.isEmpty())
				for (Var v : groupBy.getVars())
					result.addGroupBy(variables.get(v.toString()));

		// extract the ORDER BY
		List<SortCondition> orderBy = jenaQuery.getOrderBy();
		if (orderBy != null)
			if (!orderBy.isEmpty())
				for (SortCondition sc : orderBy)
					result.addOrderBy(sc);

		// extract the HAVING
		List<Expr> having = jenaQuery.getHavingExprs();
		if (having != null)
			if (!having.isEmpty()) {
				for (Expr e : having) {
					ExprFunction f = e.getFunction();
					result.addHaving(
							((ExprAggregator) f.getArg(1)).getAggregator() + " " + f.getOpName() + " " + f.getArg(2));
				}
			}

		return result;
	}

	/**
	 * Gives the position of each variable, asked in a query, in a binding. This
	 * is necessary, since PatBin sorts the triples (the original order is
	 * usually discarded).
	 * 
	 * @param query
	 *            The query used.
	 * @param table
	 *            The table used by PatBin for the compression.
	 * @return The list of positions for each variable of the query in the
	 *         binding. For example, the first integer of the list will be the
	 *         position of the first variable of the select of the query.
	 */
	public static List<Integer> getVariablesPositionInBin(SPARQLQuery query, BindingsHashTable table) {
		List<Integer> variablesPositions = new ArrayList<Integer>();
		CompressedTripleComparator comparator = new CompressedTripleComparator();
		List<String> triplesWithReplacedPredicate = new ArrayList<String>();
		String cleanTriples = query.getWhere().replace("<", "").replace(">", "");

		// sorting the triples using PatBin comparison
		for (String triple : cleanTriples.split(System.lineSeparator())) {
			String predicate = triple.split(" ")[1];
			triplesWithReplacedPredicate.add(triple.replace(predicate, table.get(predicate).toString()));
		}
		Collections.sort(triplesWithReplacedPredicate, comparator);

		// get the positions of each variable in the binding
		for (String var : query.getSelectVariables().values())
			for (String triple : triplesWithReplacedPredicate)
				if (triple.split(" ")[2].equals(var))
					variablesPositions.add(triplesWithReplacedPredicate.indexOf(triple));

		return variablesPositions;
	}
}
