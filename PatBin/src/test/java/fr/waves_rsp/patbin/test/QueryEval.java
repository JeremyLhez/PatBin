package fr.waves_rsp.patbin.test;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jena.sparql.pfunction.library.str;

import fr.waves_rsp.patbin.query.CompressedQueryResolver;

public class QueryEval {

	public static Map<String, List<String>> makeStreamMap(String patternFilePath, String bindingFilePath) {
		Map<String, List<String>> streamMap = new HashMap<String, List<String>>();

		try {
			FileReader fr1 = new FileReader(patternFilePath);
			FileReader fr2 = new FileReader(bindingFilePath);
			BufferedReader br1 = new BufferedReader(fr1);
			BufferedReader br2 = new BufferedReader(fr2);
			String line1;
			while ((line1 = br1.readLine()) != null) {
				if (streamMap.get(line1) == null) {
					List<String> l = new ArrayList<String>();
					l.add(br2.readLine());
					streamMap.put(line1, new ArrayList<String>(l));
				} else
					streamMap.get(line1).add(br2.readLine());
			}
			br1.close();
			br2.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return streamMap;
	}

	public static void main(String[] args) {
		// String streamPattern =
		// "0:336:440:(0:288:304:336:352:401:(0):402:(0):403:(0):424:(0:336):438:(0:336:460:(0:336)))";
		//
		// String queryPattern1 = "0:288:304:336:352:401";
		// String queryBinding1 = " ;xxx-xxx-xxxx;Research20;?x;?y; ";
		//
		// String queryPattern2 = "0:288:304:336:352:401:(0)";
		// String queryBinding2 = " ;?y=\"xxx-xxx-xxxx\"; ;?x; ; ; ";
		//
		// String queryPattern3 = "0:288:304:336:352:401:(0)";
		// String queryBinding3 = " ;xxx-xxx-xxxx; ;?x; ; ;?y?COUNT";

		String streamPattern = "1:2:(0:(1:6):1):3:(1:6):4:5:6";

		String queryPattern1 = "1:4:5";
		String queryBinding1 = " ;?x;?y";

		String queryPattern2 = "2:(0:(6)):4:5";
		String queryBinding2 = " ; ;?x=\"Louveciennes\";?y;?z";

		String queryPattern3 = "2:(0:(6))";
		String queryBinding3 = " ; ;?x?COUNT";

		// initialization
		// Map<String, List<String>> streamMap =
		// makeStreamMap("G:\\eval\\eval_1\\pattern.txt",
		// "G:\\eval\\eval_1\\binding.txt");
		Map<String, List<String>> streamMap = makeStreamMap("G:\\eval\\eval_10\\pattern.txt",
				"G:\\eval\\eval_10\\binding.txt");
		CompressedQueryResolver resolver = new CompressedQueryResolver(queryPattern3, queryBinding3, streamPattern);
		resolver.initialize();

		// execution
		System.out.println(streamMap.size() + " different patterns.");
		// System.out.println(streamMap.get(streamPattern).size() + " PatBin
		// forms.");
		// System.out.println();
		int minTriples = 999, maxTriples = 0, minBindings = 99999, maxBindings = 0, minSize = 9999999, maxSize = 0;
		int tmp;
		for (String key : streamMap.keySet()) {
			// System.out.println(key);

			// System.out.println(key.split(":").length + " triples.");
			tmp = key.split(":").length;
			if (tmp > 1)
				if (tmp < minTriples)
					minTriples = tmp;
			if (tmp > maxTriples)
				maxTriples = tmp;
			// System.out.println(streamMap.get(key).size() + " bindings
			// associated.");
			tmp = streamMap.get(key).size();
			if (tmp > 1)
				if (tmp < minBindings)
					minBindings = tmp;
			if (tmp > maxBindings)
				maxBindings = tmp;
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			try {
				ObjectOutputStream oos = new ObjectOutputStream(baos);
				oos.writeObject(key);
				oos.writeObject(streamMap.get(key));
				oos.close();
				// System.out.println(baos.size() + " b size.");
				tmp = baos.size();
				if (tmp > 66)
					if (tmp < minSize)
						minSize = tmp;
				if (tmp > maxSize)
					maxSize = tmp;
			} catch (IOException e) {
				e.printStackTrace();
			}
			// System.out.println();
		}
		System.out.println("min triples: " + minTriples + ", max triples: " + maxTriples);
		System.out.println("min bindings: " + minBindings + ", max bindings: " + maxBindings);
		System.out.println("min size: " + minSize + ", max size: " + maxSize);
		System.exit(0);

		long start = System.nanoTime();
		for (String binding : streamMap.get(streamPattern)) {
			Map<String, String> result = resolver.execute(binding);

			if (result != null) {
				// for (String key : result.keySet())
				// System.out.println(key + ": " + result.get(key));
				Map<String, Float> agg = resolver.aggregatesResult();
				/*
				 * if (!agg.isEmpty()) System.out.println(agg);
				 */
			}
		}
		Map<String, Float> agg = resolver.aggregatesResult();
		if (!agg.isEmpty())
			System.out.println(resolver.aggregatesResult().size());
		long end = System.nanoTime();
		System.out.println("PatBinQL: " + (end - start) / 1000000 + " millisecondes");
	}

}
