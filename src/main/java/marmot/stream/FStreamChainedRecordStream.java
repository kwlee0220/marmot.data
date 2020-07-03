package marmot.stream;

import marmot.DataSet;
import marmot.RecordSchema;
import marmot.RecordStream;
import utils.Utilities;
import utils.stream.FStream;
import utils.stream.PrependableFStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FStreamChainedRecordStream extends ChainedRecordStream {
	private final RecordSchema m_schema;
	private final FStream<? extends RecordStream> m_components;
	
	public static FStreamChainedRecordStream fromDataSets(FStream<? extends DataSet> components) {
		PrependableFStream<? extends DataSet> prep = components.toPrependable();
		RecordSchema schema = prep.peekNext()
								.map(DataSet::getRecordSchema)
								.getOrThrow(() -> new IllegalArgumentException("empty components"));
		return fromDataSets(schema, prep);
	}
	
	public static FStreamChainedRecordStream fromDataSets(RecordSchema schema,
														FStream<? extends DataSet> components) {
		return new FStreamChainedRecordStream(schema, components.map(DataSet::read));
	}
	
	public static FStreamChainedRecordStream from(FStream<? extends RecordStream> components) {
		PrependableFStream<? extends RecordStream> prep = components.toPrependable();
		RecordSchema schema = prep.peekNext()
									.map(RecordStream::getRecordSchema)
									.getOrThrow(() -> new IllegalArgumentException("empty components"));
		return from(schema, prep);
	}
	
	public static FStreamChainedRecordStream from(RecordSchema schema,
													FStream<? extends RecordStream> components) {
		return new FStreamChainedRecordStream(schema, components);
	}
	
	public FStreamChainedRecordStream(RecordSchema schema,
									FStream<? extends RecordStream> components) {
		Utilities.checkNotNullArgument(schema, "schema is null");
		Utilities.checkNotNullArgument(components, "components is null");
		
		m_schema = schema;
		m_components = components;
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}

	@Override
	protected RecordStream getNextRecordStream() {
		return m_components.next().getOrNull();
	}
}