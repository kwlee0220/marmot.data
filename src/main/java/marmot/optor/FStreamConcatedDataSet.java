package marmot.optor;

import marmot.DataSet;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.stream.FStreamChainedRecordStream;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FStreamConcatedDataSet implements DataSet {
	private final RecordSchema m_schema;
	private final FStream<? extends DataSet> m_datasets;

	public FStreamConcatedDataSet(RecordSchema schema, FStream<? extends DataSet> datasets) {
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
