package fr.waves_rsp.patbin.test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;

import fr.waves_rsp.patbin.query.QueryResolver;

public class WavesExample {
	public static final String SEP = System.getProperty("line.separator");

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

		String streamExtract = "@prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#> ." + SEP
				+ "@prefix cuahsi:<http://his.cuahsi.org/ontology/cuahsi#>  ." + SEP
				+ "@prefix waves:<http://www.waves.org/ontology#> ." + SEP + "_:11 waves:id \"QSHC1\" ;" + SEP
				+ "	waves:date \"30/03/2017\" ;" + SEP + "	cuahsi:measure _:12 ." + SEP + "_:12 rdfs:value 4.7 .";
		// String streamExtract = "@prefix
		// rdfs:<http://www.w3.org/2000/01/rdf-schema#> ." + SEP
		// + "@prefix cuahsi:<http://his.cuahsi.org/ontology/cuahsi#> ." + SEP
		// + "@prefix waves:<http://www.waves.org/ontology#> ." + SEP
		// + "@prefix wgs:<http://www.w3.org/2003/01/geo/wgs84_pos#> ." + SEP
		// + "_:11 waves:id \"QSHC1\" ;" + SEP
		// + " waves:date \"30/03/2017\" ;" + SEP
		// + " cuahsi:measure _:12 ;" + SEP
		// + " wgs:hasLocation \"Platform1\" ." + SEP
		// + "_:12 rdfs:value 4.7 .";

		String query = "SELECT ?x (COUNT(?v) AS ?result)" + SEP + "WHERE {" + SEP
				+ "?s <http://www.waves.org/ontology#id> ?x ." + SEP
				+ "?s <http://www.w3.org/2003/01/geo/wgs84_pos#hasLocation> \"Platform1\" ." + SEP
				+ "?s <http://his.cuahsi.org/ontology/cuahsi#measure> ?y ." + SEP
				+ "?y <http://www.w3.org/2000/01/rdf-schema#value> ?v ." + SEP + "} " + SEP + "GROUP BY ?x";
//		String query = "SELECT ?x ?v WHERE { ?s <http://www.waves.org/ontology#id> ?x . ?s"
//				+ "<http://www.w3.org/2003/01/geo/wgs84_pos#hasLocation> \"Platform1\" ."
//				+ "?s <http://his.cuahsi.org/ontology/cuahsi#measure> ?y . ?y"
//				+ "<http://www.w3.org/2000/01/rdf-schema#value> ?v . }";
		
		String streamPattern = "38:40:56:(42)";
//		String streamPattern = "38:40:54:56:(42)";
		
		String staticEndpoint = "http://localhost:3030/waves-static/query";
		
		bindingsTable.put("http://www.waves.org/ontology#date", 38);
		bindingsTable.put("http://www.waves.org/ontology#id", 40);
		bindingsTable.put("http://www.w3.org/2000/01/rdf-schema#value", 42);
		bindingsTable.put("http://www.w3.org/2003/01/geo/wgs84_pos#hasLocation", 54);
		bindingsTable.put("http://his.cuahsi.org/ontology/cuahsi#measure", 56);

		statisticMap.put(40, 5);
		statisticMap.put(54, 5);
		statisticMap.put(56, 5);
		
		QueryResolver resolver = new QueryResolver(query, streamPattern, bindingsTable, statisticMap, staticEndpoint);	
		Map<String, String> result = resolver.processQuery(streamExtract);

		if (result != null)
			for (String key : result.keySet())
				System.out.println(key + ": " + result.get(key));

		Map<String, String> aggregates = resolver.processAggregates();

		if (aggregates != null)
			for (String key : aggregates.keySet())
				System.out.println(key + ": " + aggregates.get(key));
	}

}
