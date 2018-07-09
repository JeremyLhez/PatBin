package fr.waves_rsp.patbin.test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;

import fr.waves_rsp.patbin.PatBin;
import fr.waves_rsp.patbin.compressor.PatBinCompressor;

public class CompressionTest {
	public static final String EXAMPLE_DIRECTORY_PATH = "C:\\Users\\jerem_000\\workspace\\PatBin\\src\\test\\resources\\";

	public static void main(String[] args) throws UnsupportedEncodingException {
		PatBinCompressor patBinCompressor = new PatBinCompressor();
		FileInputStream fis;

		try {
			Properties properties = new Properties();
			properties.load(new FileInputStream("src\\log4j.properties"));
			PropertyConfigurator.configure(properties);
			
			fis = new FileInputStream(EXAMPLE_DIRECTORY_PATH + "waves_example_10.turtle");

			PatBin compressedForm = patBinCompressor.compressFile(fis, "TTL");
			System.out.println();
			System.out.println("pattern: " + compressedForm.getPattern());
			System.out.println("binding: " + compressedForm.getBinding());
			System.out.println();
			System.out.println(patBinCompressor.getTable());
			//System.out.println(patBin.uncompress(compressedForm, 0));

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
