package marmot.optor;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.DataSet;
import marmot.DefaultRecord;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.stream.AbstractRecordStream;
import marmot.support.ColumnVariableResolverFactory;
import marmot.support.DataUtils;
import marmot.support.RecordScript;
import marmot.support.RecordScriptExecution;
import utils.Utilities;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FilterScript implements DataSet {
	private static final Logger s_logger = LoggerFactory.getLogger(FilterScript.class);
	
	private final DataSet m_input;
	private final RecordScript m_script;

	public FilterScript(DataSet input, RecordScript script) {
		Utilities.checkNotNullArgument(script, "predicate is null");
		
		m_input = input;
		m_script = script;
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_input.getRecordSchema();
	}

	@Override
	public RecordStream read() {
		return new StreamImpl(m_input.read(), m_script);
	}
	
	@Override
	public String toString() {
		return String.format("%s: predicate='%s'", getClass().getSimpleName(), m_script);
	}
	
	private static class StreamImpl extends AbstractRecordStream {
		private final RecordStream m_input;
		private final Record m_inputRecord;
		private final RecordScriptExecution m_filterExec;
		private final ColumnVariableResolverFactory m_vrFact;
		
		private StreamImpl(RecordStream input, RecordScript script) {
			m_input = input;
			m_inputRecord = DefaultRecord.of(input.getRecordSchema());
			
			m_filterExec = RecordScriptExecution.of(script);
			Map<String,Object> args = m_filterExec.getArgumentAll();
			m_vrFact = new ColumnVariableResolverFactory(input.getRecordSchema(), args).readOnly(true);
			m_filterExec.initialize(m_vrFact);
		}

		@Override
		protected void closeInGuard() throws Exception {
			m_input.close();
		}

		@Override
		public RecordSchema getRecordSchema() {
			return m_input.getRecordSchema();
		}

		@Override
		public boolean next(Record record) {
			while ( m_input.next(record) ) {
				try {
					if ( test(record) ) {
						return true;
					}
				}
				catch ( Throwable e ) {
					getLogger().warn("ignored transform failure: op=" + this + ", cause=" + e);
				}
			}
			
			return false;
		}

		private boolean test(Record record) {
			m_vrFact.bind(record);
			try {
				return DataUtils.asBoolean(m_filterExec.execute(m_vrFact));
			}
			catch ( Throwable e ) {
				if ( getLogger().isDebugEnabled() ) {
					String msg = String.format("fails to evaluate the predicate: '%s, record=%s",
												m_filterExec, record);
					getLogger().warn(msg);
				}
				return false;
			}
		}
	}
}