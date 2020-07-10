package marmot.avro;

import java.io.File;

import marmot.RecordReader;
import marmot.RecordWriter;
import marmot.dataset.AbstractDataSetServer;
import marmot.dataset.Catalog;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LfsAvroDataSetServer extends AbstractDataSetServer {
	public LfsAvroDataSetServer(Catalog catalog) {
		super(catalog);
	}

	@Override
	protected RecordReader getRecordReader(String filePath) {
		return new AvroFileRecordReader(new File(filePath));
	}

	@Override
	protected RecordWriter getRecordWriter(String filePath) {
		return new AvroFileRecordWriter(new File(filePath));
	}
}
