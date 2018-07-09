package fr.waves_rsp.patbin.test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;

import fr.waves_rsp.patbin.query.SPARQLQuery;
import fr.waves_rsp.patbin.query.SPARQLQueryManager;

public class QueryAggregatorsTest {
	public static final String SEP = System.getProperty("line.separator");

	public static void main(String[] args) {
		try {
			// load properties
			Properties properties = new Properties();
			properties.load(new FileInputStream("src\\log4j.properties"));
			PropertyConfigurator.configure(properties);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		String query = "SELECT ?x (COUNT(?v) AS ?result)" + SEP + "WHERE {" + SEP
				+ "?s <http://www.waves.org/ontology#id> ?x ." + SEP
				+ "?s <http://www.w3.org/2003/01/geo/wgs84_pos#hasLocation> \"Platform1\" ." + SEP
				+ "?s <http://his.cuahsi.org/ontology/cuahsi#measure> ?y ." + SEP
				+ "?y <http://www.w3.org/2000/01/rdf-schema#value> ?v ." + SEP + "}" + SEP + "GROUP BY ?x" + SEP
				+ "HAVING (COUNT(?v) > 5)" + SEP + "ORDER BY ASC(?x)";
		
		System.out.println(query);
		System.out.println();
		
		SPARQLQuery queryElements = SPARQLQueryManager.extractQueryElements(query);
		System.out.println(queryElements);
	}

}
