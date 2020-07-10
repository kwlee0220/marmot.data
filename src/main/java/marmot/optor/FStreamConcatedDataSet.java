package marmot.optor;

import marmot.RecordReader;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.stream.FStreamChainedRecordStream;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FStreamConcatedDataSet implements RecordReader {
	private final RecordSchema m_schema;
	private final FStream<? extends RecordReader> m_datasets;

	public FStreamConcatedDataSet(RecordSchema schema, FStream<? extends RecordReader> datasets) {
		m_schema = schema;
		m_datasets = datasets;
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}

	@Override
	public RecordStream read() {	
		return FStreamChainedRecordStream.fromDataSets(m_schema, m_datasets);
	}
}
