package fr.waves_rsp.patbin.test;

import fr.waves_rsp.patbin.PatBin;
import fr.waves_rsp.patbin.generator.QueryGenerator;

public class GeneratorTest {

	public static void main(String[] args) {
		PatBin stream = new PatBin("1:(2:3):4:(5:6:(7:8):9:(7:8))", "a;b;c;d;e;f;var1;h;i;var2;k");
		QueryGenerator generator = new QueryGenerator(stream);
		
		System.out.println(stream);
		
		generator.buildQuery();
		
		System.out.println(generator.getGeneratedQuery());
	}
}
