package fr.waves_rsp.patbin.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jena.query.SortCondition;
import org.apache.jena.sparql.expr.aggregate.Aggregator;

import fr.waves_rsp.patbin.compressor.BindingsHashTable;
import fr.waves_rsp.patbin.query.continuous.SPARQLAggregate;
import fr.waves_rsp.patbin.util.CompressedTripleComparator;

/**
 * Representation of the different elements of a SPARQL query to be use by
 * PatBin. The query is decomposed to identify clearly the triples in the WHERE
 * clause and the variables in the select.
 * 
 * @author Jeremy Lhez
 *
 */
public class SPARQLQuery {
	private String where, originalQuery;
	// variable -> node for the WHERE clause
	private Map<String, String> whereVariables;
	// variable -> node for the WHERE clause
	private Map<String, String> selectVariables;
	private List<String> groupBy;
	private Map<Aggregator, String> aggregates;
	// position in the binding as key
	private Map<Integer, SPARQLAggregate> aggregateValues;
	private List<SortCondition> orderBy;
	private List<String> having;

	public SPARQLQuery(String originalQuery, String where, Map<String, String> whereVariables) {
		this.where = where;
		this.originalQuery = originalQuery;
		this.whereVariables = whereVariables;
		this.selectVariables = new HashMap<String, String>();
		this.groupBy = new ArrayList<String>();
		this.aggregates = new HashMap<Aggregator, String>();
		this.aggregateValues = new HashMap<Integer, SPARQLAggregate>();
		this.orderBy = new ArrayList<SortCondition>();
		this.having = new ArrayList<String>();
	}

	public String getWhere() {
		return where;
	}

	public String getOriginalQuery() {
		return originalQuery;
	}

	public void addSelectVariable(String variable, String node) {
		this.selectVariables.put(variable, node);
	}

	public Map<String, String> getSelectVariables() {
		return selectVariables;
	}

	public Map<String, String> getWhereVariables() {
		return whereVariables;
	}

	public boolean hasGroupBy() {
		return !groupBy.isEmpty();
	}

	public List<String> getGroupBy() {
		return groupBy;
	}

	public void addGroupBy(String group) {
		this.groupBy.add(group);
	}

	public boolean hasAggregate() {
		return !aggregates.isEmpty();
	}

	public Map<Aggregator, String> getAggregate() {
		return aggregates;
	}

	public Map<Integer, SPARQLAggregate> getAggregatesValues() {
		return aggregateValues;
	}

	public void addAggregate(String as, Aggregator agg) {
		this.aggregates.put(agg, as);
	}

	/**
	 * Initializes the Map storing the values used in the aggregates.
	 * 
	 * @param table
	 *            The Bindings Table used for the query; will be used to sort
	 *            the triples, thus getting the order of the predicates in the
	 *            PatBin format.
	 */
	public void initializeAggregateValues(BindingsHashTable table) {
		CompressedTripleComparator comparator = new CompressedTripleComparator();
		List<String> triplesWithReplacedPredicate = new ArrayList<String>();
		String cleanTriples = where.replace("<", "").replace(">", "");

		// sorting the triples using PatBin comparison (to get the good order)
		for (String triple : cleanTriples.split(System.lineSeparator())) {
			String predicate = triple.split(" ")[1];
			triplesWithReplacedPredicate.add(triple.replace(predicate, table.get(predicate).toString()));
		}
		Collections.sort(triplesWithReplacedPredicate, comparator);

		// for each aggregate
		for (Aggregator agg : aggregates.keySet()) {
			// get the node corresponding to its variable
			String node = whereVariables.get(SPARQLQuery.getAggregateVariable(agg.toString()));
			String aggString = agg.toString();
			for (String triple : triplesWithReplacedPredicate)
				// once we find the corresponding triple, we have the
				// position
				if (triple.split(" ")[2].equals(node)) {
					SPARQLAggregate sprqlAgg = new SPARQLAggregate(aggString.substring(0, aggString.indexOf("(")));
					aggregateValues.put(triplesWithReplacedPredicate.indexOf(triple), sprqlAgg);
				}
		}
	}

	public boolean hasOrderBy() {
		return !orderBy.isEmpty();
	}

	public List<String> getOrderByVariables() {
		List<String> order = new ArrayList<String>();
		for (SortCondition c : orderBy)
			if (c.getDirection() == -1)
				order.add("DESC " + selectVariables.get(c.getExpression()) + " ");
			else if (c.getDirection() == 1)
				order.add("ASC " + selectVariables.get(c.getExpression()) + " ");

		return order;
	}

	public void addOrderBy(SortCondition order) {
		this.orderBy.add(order);
	}

	public boolean hasHaving() {
		return !having.isEmpty();
	}

	public List<String> getHaving() {
		return having;
	}

	public void addHaving(String having) {
		this.having.add(having);
	}

	public static String getAggregateVariable(String expression) {
		String res;

		res = expression.substring(expression.indexOf('?'));
		res = res.substring(0, res.indexOf(")"));

		return res;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();

		builder.append("SELECT: ");
		for (String key : selectVariables.keySet())
			builder.append(selectVariables.get(key));
		builder.append(System.getProperty("line.separator"));

		builder.append("WHERE: ");
		builder.append(System.getProperty("line.separator"));
		builder.append(where);
		builder.append(System.getProperty("line.separator"));

		if (!groupBy.isEmpty()) {
			builder.append("GROUP BY: ");
			for (String v : groupBy)
				builder.append(v + " ");
			builder.append(System.getProperty("line.separator"));
		}

		if (!aggregates.isEmpty()) {
			builder.append("AGGREGATES: ");
			for (Aggregator key : aggregates.keySet()) {
				String[] splitAgg = key.toString().split("\\(");

				builder.append(splitAgg[0] + " " + whereVariables.get(getAggregateVariable(splitAgg[1])) + " AS "
						+ aggregates.get(key) + " ");
			}
			builder.append(System.getProperty("line.separator"));
		}

		if (!orderBy.isEmpty()) {
			builder.append("ORDER BY: ");
			for (SortCondition sc : orderBy)
				if (sc.getDirection() == -1)
					builder.append("DESC " + whereVariables.get(sc.getExpression().toString()) + " ");
				else if (sc.getDirection() == 1)
					builder.append("ASC " + whereVariables.get(sc.getExpression().toString()) + " ");
			builder.append(System.getProperty("line.separator"));
		}

		if (!having.isEmpty()) {
			builder.append("HAVING: ");
			for (String expr : having) {
				String var = getAggregateVariable(expr);
				builder.append(expr.replace(var, whereVariables.get(var)) + " ");
			}
			builder.append(System.getProperty("line.separator"));
		}

		return builder.toString();
	}
}
