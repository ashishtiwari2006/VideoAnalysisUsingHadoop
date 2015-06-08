package job1;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;

import javax.imageio.ImageIO;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


	public class VideoSceneMapper extends
			Mapper<Text, BytesWritable, Text, CustomImageReaderWriter> {
		
		Log log = LogFactory.getLog(VideoSceneMapper.class);

		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {

			String[] keyParts = key.toString().split("_");
			Text newSceneKey = new Text(keyParts[1] + "_" + keyParts[2]);
			
			int imgLenth = new Integer(keyParts[3]);			
			
			
			int noBytes = value.getLength();
			
			
		
			
			byte[] rawdata = value.getBytes();
		
			byte[] data = Arrays.copyOfRange(rawdata,  0, noBytes); 
			
			
		//	byte[] data = value.copyBytes();

			BufferedImage image = ImageIO.read(new ByteArrayInputStream(data));

			int w = image.getWidth();
			int h = image.getHeight();

			double avgRed = 0.0;
			double avgGreen = 0.0;
			double avgBlue = 0.0;
			int red = 0;
			int green = 0;
			int blue = 0;
			int alpha = 0;
			int pixel = 0;

			for (int i = 0; i < h; i++) {
				for (int j = 0; j < w; j++) {
					
					pixel = image.getRGB(j, i);
					
					 red = (pixel) & 0xFF;
					 green = (pixel>>8)&0xFF;
					 blue = (pixel>>16)&0xFF;
					 alpha = (pixel>>24)&0xFF;				
					
					avgRed = avgRed + red;
					avgGreen = avgGreen + green;
					avgBlue = avgBlue + blue;
				}
			}

			int totalPixel = h * w;

			avgRed = (double) avgRed / (double) totalPixel;
			avgGreen = (double) avgGreen / (double) totalPixel;
			avgBlue = (double) avgBlue / (double) totalPixel;
			
	/*		log.info("first no of Bytes: "+imgLenth+" second no of Bytes got from sequence file: "+noBytes);
			log.info(" avgRed: "+avgRed+" avgGreen: "+avgGreen+" avgBlue: "+avgBlue + " totalPixel: "+totalPixel);*/

			
			CustomImageReaderWriter val = new CustomImageReaderWriter();

			val.size = value.getBytes().length;
			val.imgIndex = new Long(keyParts[0]);
			val.avgRed = avgRed;
			val.avgGreen = avgGreen;
			val.avgBlue = avgBlue;
	//		val.data = value.getBytes();

			context.write(newSceneKey, val);

		}
	}