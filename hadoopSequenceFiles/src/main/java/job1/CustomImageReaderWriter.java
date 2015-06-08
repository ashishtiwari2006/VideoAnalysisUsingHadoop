package job1;



import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class CustomImageReaderWriter implements Writable {
	// Some data

	public int size;
	public long imgIndex;
	public double avgRed;
	public double avgGreen;
	public double avgBlue;

//	public byte[] data;

	public void write(DataOutput out) throws IOException {

		out.writeInt(size);
		out.writeLong(imgIndex);
		out.writeDouble(avgRed);
		out.writeDouble(avgGreen);
		out.writeDouble(avgBlue);
/*
		for (Byte bt : data) {
			out.writeByte(bt);
		}*/
	}

	public void readFields(DataInput in) throws IOException {
		size = in.readInt();
		imgIndex = in.readLong();
		avgRed = in.readDouble();
		avgGreen = in.readDouble();
		avgBlue = in.readDouble();
	/*	data = new byte[size];
		for (int i = 0; i < size; i++) {
			data[i] = in.readByte();
		}*/

	}

	public static CustomImageReaderWriter read(DataInput in) throws IOException {
		CustomImageReaderWriter w = new CustomImageReaderWriter();
		w.readFields(in);
		return w;
	}
}
