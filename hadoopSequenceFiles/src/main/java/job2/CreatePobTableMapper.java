package job2;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CreatePobTableMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable key, Text value, Context context) throws IOException,
			InterruptedException {

			

		String[] keyPartsAndrgb = value.toString().trim().split("\\s+");
		
		String sNo = keyPartsAndrgb[0].trim();
		String isComm = sNo.trim().split("_")[1];
		
		String red =  keyPartsAndrgb[1].trim();
		String grn =  keyPartsAndrgb[2].trim();
		String blue =  keyPartsAndrgb[3].trim();
		
		
		String [] rgbRange = new String [3];
		

		
		Double remainder = 0.0;

		Double r = new Double(red);
		remainder = r % 10;
		int r_SRange = (int) (r - remainder);
		int r_ERange = r_SRange + 9;
		rgbRange[0] = "R_"+isComm+"_"+r_SRange + "-" + r_ERange;

		
		Double g = new Double(grn);
		remainder = g % 10;
		int g_SRange = (int) (g - remainder);
		int g_ERange = g_SRange + 9;		
		rgbRange[1]= "G_"+isComm+"_"+g_SRange + "-" + g_ERange;
		
		
		
		Double b = new Double(blue);
		remainder = b % 10;
		int b_SRange = (int) (b - remainder);
		int b_ERange = b_SRange + 9;		
		rgbRange[2] = "B_"+isComm+"_"+b_SRange + "-" + b_ERange;
		
		for (String st : rgbRange) {
			context.write(new Text(st), new IntWritable(1));		
		}
		
		 
	

	}

}
