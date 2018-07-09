package fr.waves_rsp.patbin.test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;

import fr.waves_rsp.patbin.query.QueryResolver;
import fr.waves_rsp.patbin.test.util.StreamFromFile;

public class FinancialExample {
	public static final String SEP = System.getProperty("line.separator");

	/**
	 * Test on only one stream sample.
	 * 
	 * @param resolver
	 *            The query resolver to be used.
	 * @param streamExtract
	 *            The stream extract to be used.
	 */
	public static void processSimple(QueryResolver resolver, String streamExtract) {
		Map<String, String> result = resolver.processQuery(streamExtract);

		if (result != null)
			for (String key : result.keySet())
				System.out.println(key + ": " + result.get(key));

		Map<String, String> aggregates = resolver.processAggregates();

		if (aggregates != null)
			for (String key : aggregates.keySet())
				System.out.println(key + ": " + aggregates.get(key));
	}

	/**
	 * Test on multiple stream samples.
	 * 
	 * @param resolver
	 *            The query resolver to be used.
	 * @param fileStreamPath
	 *            The path to the file containing the stream extracts.
	 */
	public static void processMultiple(QueryResolver resolver, String fileStreamPath) {
		List<String> samples = StreamFromFile.extractSamplesFromFile(fileStreamPath);
		int i = 1;

		for (String streamExtract : samples) {
			Map<String, String> result = resolver.processQuery(streamExtract);

			System.out.println("#" + i);
			if (result != null)
				for (String key : result.keySet())
					System.out.println(key + ": " + result.get(key));
			System.out.println();
			i++;
		}

		Map<String, String> aggregates = resolver.processAggregates();

		if (aggregates != null)
			for (String key : aggregates.keySet())
				System.out.println(key + ": " + aggregates.get(key));
	}

	public static void main(String[] args) {

		// JENA logs configuration
		try {
			Properties properties = new Properties();
			properties.load(new FileInputStream("src\\log4j.properties"));
			PropertyConfigurator.configure(properties);
		} catch (FileNotFoundException e) {
			System.out.println("Jena log4j properties file not found.");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("Unable to access Jena log4j properties file.");
			e.printStackTrace();
		}

		Map<String, Integer> bindingsTable = new HashMap<String, Integer>();
		Map<Integer, Integer> statisticMap = new HashMap<Integer, Integer>();

		String streamExtract = "@prefix fin:<http://www.financial.org/ontology#> ." + SEP 
				+ "_:1 fin:origin _:2 ;" + SEP
				+ "fin:debit _:3 ." + SEP 
				+ "_:2 fin:id \"BANK#4016\" ;" + SEP 
				+ "fin:label \"Postal Bank\" ." + SEP
				+ "_:3 fin:money _:4 ;" + SEP 
				+ "fin:input _:5 ;" + SEP 
				+ "fin:output _:6 ." + SEP
				+ "_:4 fin:amount 450.00 ;" + SEP 
				+ "fin:unit \"US$\" ." + SEP 
				+ "_:5 fin:name \"Dupont\" ;" + SEP
				+ "fin:account \"CCP XXX\" ." + SEP 
				+ "_:6 fin:name \"Miller\" ;" + SEP 
				+ "fin:account \"CCP YYY\" .";

		// query
		// somme d'argent débitée par la banque 4016, pour chaque devise
		String query = "SELECT (SUM(?v) AS ?sum) ?w" + SEP + "WHERE {" + SEP
				+ "?x <http://www.financial.org/ontology#origin> ?y ." + SEP
				+ "?y <http://www.financial.org/ontology#id> \"BANK#4016\" ." + SEP
				+ "?x <http://www.financial.org/ontology#debit> ?z ." + SEP
				+ "?z <http://www.financial.org/ontology#money> ?u ." + SEP
				+ "?u <http://www.financial.org/ontology#amount> ?v ." + SEP
				+ "?u <http://www.financial.org/ontology#unit> ?w ." + SEP 
				+ "}" + SEP 
				+ "GROUP BY ?w";
		 		//+ SEP + "HAVING SUM(?v) > 10000";

		// stream pattern
		String streamPattern = "31:(5:8):40:(44:(49:53):66:(12:74):67:(12:74))";

		// static endpoint
		String staticEndpoint = "http://localhost:3030/financial-example/query";

		// initialization of the bindings table
		bindingsTable.put("http://www.financial.org/ontology#origin", 31);
		bindingsTable.put("http://www.financial.org/ontology#label", 5);
		bindingsTable.put("http://www.financial.org/ontology#id", 8);
		bindingsTable.put("http://www.financial.org/ontology#debit", 40);
		bindingsTable.put("http://www.financial.org/ontology#money", 44);
		bindingsTable.put("http://www.financial.org/ontology#amount", 49);
		bindingsTable.put("http://www.financial.org/ontology#unit", 53);
		bindingsTable.put("http://www.financial.org/ontology#input", 66);
		bindingsTable.put("http://www.financial.org/ontology#output", 67);
		bindingsTable.put("http://www.financial.org/ontology#name", 12);
		bindingsTable.put("http://www.financial.org/ontology#account", 74);

		// initialization of the statistics' map
		statisticMap.put(5, 5);
		statisticMap.put(8, 5);
		statisticMap.put(12, 5);
		statisticMap.put(74, 5);

		QueryResolver resolver = new QueryResolver(query, streamPattern, bindingsTable, statisticMap, staticEndpoint);

		processSimple(resolver, streamExtract);
		//processMultiple(resolver, "src\\test\\resources\\stream\\financial.ttl");
	}

}
