package marmot.dataset;

import com.vividsolutions.jts.geom.Envelope;

import marmot.RecordReader;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.RecordWriter;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface DataSet extends RecordReader, RecordWriter {	
	public default String getId() {
		return getDataSetInfo().getId();
	}

	public default RecordSchema getRecordSchema() {
		return getDataSetInfo().getRecordSchema();
	}


	public default Envelope getBounds() {
		return getDataSetInfo().getBounds();
	}

	public default long getRecordCount() {
		return getDataSetInfo().getRecordCount();
	}
	
	public DataSetInfo getDataSetInfo();

	@Override
	public RecordStream read();

	@Override
	public void write(RecordStream stream);
	
	public void append(RecordStream stream);
}
