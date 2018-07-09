package fr.waves_rsp.patbin.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;

import fr.waves_rsp.patbin.PatBin;
import fr.waves_rsp.patbin.compressor.BindingsHashTable;
import fr.waves_rsp.patbin.compressor.PatBinCompressor;

public class LUBMEval {
	public static void main(String[] args) {

		try {
			Properties properties = new Properties();
			properties.load(new FileInputStream("src\\log4j.properties"));
			PropertyConfigurator.configure(properties);

			// initialize binding table
			long start = System.nanoTime();
			//BindingsHashTable table = new BindingsHashTable();
			//File f = new File("G:\\eval\\lubm-mail\\lubm_propertiesURL2Id.dct");
			//File f = new File("G:\\eval\\eval_10\\lubm_encoding.txt");
			//table.loadFromFile(f);

			// initialize PatBin
			PatBinCompressor patBinCompressor = new PatBinCompressor();
			//patBinCompressor.setBindingTable(table);
			long end = System.nanoTime();
			System.out.println("initialisation: " + (end - start) / 1000 + " microsecondes");
			//FileInputStream fis = new FileInputStream("G:\\eval\\eval_1\\lubm1.ttl");
			//FileInputStream fis = new FileInputStream("G:\\eval\\eval_10\\univ_lubm10.nt");
			FileInputStream fis = new FileInputStream("G:\\eval\\eval_0\\sectors-ssn.ttl");
			
			start = System.nanoTime();
			PatBin compressedForm = patBinCompressor.compressFile(fis, "TTL");
			end = System.nanoTime();
			System.out.println("compression: " + (end - start) / 1000 + " microsecondes");

			// write the result into the files
			PrintWriter pwP = new PrintWriter("G:\\eval\\eval_0\\pattern2.txt");
			PrintWriter pwB = new PrintWriter("G:\\eval\\eval_0\\binding2.txt");

			pwP.write(compressedForm.getPattern());
			pwB.write(compressedForm.getBinding());

			pwP.close();
			pwB.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
