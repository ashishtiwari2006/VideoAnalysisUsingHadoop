package job1;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class VideoSceneReducer extends
		Reducer<Text, CustomImageReaderWriter, Text, Text> {
	
	Log log = LogFactory.getLog(VideoSceneReducer.class);

	public void reduce(Text key, Iterable<CustomImageReaderWriter> values,
			Context context) throws IOException, InterruptedException {

	//	String imagesInfo = "";
		
		double avgRed = 0.0;
		double avgGreen = 0.0;
		double avgBlue = 0.0;
		
		double sRed = 0.0;
		double sGreen = 0.0;
		double sBlue = 0.0;

		int noOfImages = 0;

		for (CustomImageReaderWriter imageIndexAndData : values) {
			noOfImages++;	
			sRed = sRed + imageIndexAndData.avgRed;
			sGreen = sGreen + imageIndexAndData.avgGreen;
	        sBlue = sBlue + imageIndexAndData.avgBlue;
		}
		

	//	log.info("in reducer  sumAvgRed: "+sRed+" sumAvgGreen: "+sGreen+" sumAvgBlue: "+sBlue + " noOfImages: "+noOfImages);

		avgRed = sRed / noOfImages;
		avgGreen = sGreen / noOfImages;
		avgBlue = sBlue / noOfImages;
		
//		log.info(" in reducer  avgRed: "+avgRed+" avgGreen: "+avgGreen+" avgBlue: "+avgBlue + " noOfImages: "+noOfImages);
		

		// In the result file the key will be again the image file path.
		context.write(key, new Text(avgRed + " "+avgGreen +" "+ avgBlue));
	}
}