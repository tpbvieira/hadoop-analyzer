package org.apache.hadoop.analyzer;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;

public class BestStats {

	public static void main(String args[]){
		
		DescriptiveStatistics stats = new DescriptiveStatistics();		
		
		try{
			FileInputStream fstream = new FileInputStream("/home/thiago/tmp/experiment/1st_experiment/tmp.dat");
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;
			int i = 0;
			while ((strLine = br.readLine()) != null)   {
				stats.addValue(Double.parseDouble(strLine));
				System.out.println((++i) + " - " + stats.getMean() + " - " + stats.getStandardDeviation());
			}
			in.close();
		}catch (Exception e){
			e.printStackTrace();
		}
	}

}