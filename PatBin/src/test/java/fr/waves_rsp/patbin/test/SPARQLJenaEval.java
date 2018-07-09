package fr.waves_rsp.patbin.test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.log4j.PropertyConfigurator;

public class SPARQLJenaEval {

	public static void main(String[] args) {

		try {
			// JENA logger
			Properties properties = new Properties();
			properties.load(new FileInputStream("src\\log4j.properties"));
			PropertyConfigurator.configure(properties);

			Model model = ModelFactory.createDefaultModel();
			//model.read("G:\\eval\\eval_10\\univ_lubm10.nt");
			model.read("G:\\eval\\eval_0\\sectors-ssn.ttl");

			/*String queryString1 = "SELECT ?x ?y WHERE {"
					+ " ?a <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?b ."
					+ " ?a <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#telephone> \"xxx-xxx-xxxx\" ."
					+ " ?a <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#researchInterest> \"Research20\" ."
					+ " ?a <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name> ?x ."
					+ " ?a <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress> ?y ."
					+ " ?a <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom> ?c ."
					+ " }";
			String queryString2 = "SELECT ?x ?v WHERE {"
					+ " ?a <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?b ."
					+ " ?a <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#telephone> ?v ."
					+ " ?a <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#researchInterest> ?c ."
					+ " ?a <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name> ?x ."
					+ " ?a <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress> ?y ."
					+ " ?a <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom> ?d ."
					+ " ?d <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?z ."
					+ " FILTER regex(str(?v), \"xxx-xxx-xxxx\") ."
					+ " }";
			String queryString3 = "SELECT ?x (COUNT(?z) AS ?count) WHERE {"
					+ " ?a <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?b ."
					+ " ?a <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#telephone> \"xxx-xxx-xxxx\" ."
					+ " ?a <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#researchInterest> ?c ."
					+ " ?a <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name> ?x ."
					+ " ?a <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress> ?y ."
					+ " ?a <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom> ?d ."
					+ " ?d <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?z ."
					+ " } GROUP BY ?x";*/
			
			String queryString1 = "SELECT ?x ?y "
					+ "WHERE {"
					+ "?a <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.oclc.org/NET/ssnx/ssn#Sensor> ."
					+ "?a <http://www.w3.org/2003/01/geo/wgs84_pos#lat> ?x ."
					+ "?a <http://www.w3.org/2003/01/geo/wgs84_pos#long> ?y ."
					+ "}";
			String queryString2 = "SELECT ?x ?y ?z "
					+ "WHERE {"
					+ "?a <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.oclc.org/NET/ssnx/ssn#Sensor> ."
					+ "?a <http://www.w3.org/2000/01/rdf-schema#label> ?x ."
					+ "?a <http://www.w3.org/2003/01/geo/wgs84_pos#lat> ?y ."
					+ "?a <http://www.w3.org/2003/01/geo/wgs84_pos#long> ?z ."
					+ "FILTER regex(?x, \"QS\") ."
					+ "}";
			String queryString3 = "SELECT (COUNT(?x) AS ?count) "
					+ "WHERE {"
					+ "?a <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.oclc.org/NET/ssnx/ssn#Sensor> ."
					+ "?a <http://www.w3.org/2000/01/rdf-schema#label> ?x ."
					+ "}";
			long start = System.nanoTime();
			Query query = QueryFactory.create(queryString3);
			try (QueryExecution qexec = QueryExecutionFactory.create(query, model)) {
				start = System.nanoTime();
				ResultSet results = qexec.execSelect();
				ResultSetFormatter.out(System.out, results, query);
			}
			long end = System.nanoTime();
			System.out.println("JENA simple: " + (end - start) / 1000000 + " millisecondes");
		} catch (FileNotFoundException e) {
			System.out.println("Can't find property file src\\log4j.properties");
		} catch (IOException e) {
			System.out.println("Can't read property file src\\log4j.properties");
		}
	}

}
