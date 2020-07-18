package marmot.optor;

import marmot.DefaultRecord;
import marmot.Record;
import marmot.RecordReader;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.optor.support.colexpr.ColumnSelector;
import marmot.optor.support.colexpr.ColumnSelectorFactory;
import marmot.stream.AbstractRecordStream;
import utils.Utilities;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ProjectedReader implements RecordReader {
	private final RecordReader m_input;
	private final String m_columnSelection;
	
	private final RecordSchema m_schema;
	private final ColumnSelector m_selector;

	/**
	 * 주어진 컬럼 이름들로 구성된 projection 스트림 연산자를 생성한다.
	 * 연산 수행 결과로 생성된 레코드 세트는 입력 레코드 세트에 포함된 각 레코드들에 대해
	 * 주어진 이름의 컬럼만으로 구성된 레코드들로 구성된다. 
	 * 
	 * @param	columnSelection	projection 연산에 사용될 컬럼들의 이름 배열.
	 */
	public ProjectedReader(RecordReader input, String columnSelection) {
		Utilities.checkArgument(columnSelection != null, "Column seelection expression is null");

		m_input = input;
		m_columnSelection = columnSelection;
		m_selector = ColumnSelectorFactory.create(input.getRecordSchema(), m_columnSelection);
		m_schema = m_selector.getRecordSchema();
	}
	
	public ProjectedReader(RecordReader input, MultiColumnKey keys) {
		this(input, keys.streamKeyColumns().map(KeyColumn::name).join(","));
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	public String getColumnSelection() {
		return m_columnSelection;
	}

	@Override
	public RecordStream read() {
		return new StreamImpl(m_input.read(), m_selector);
	}
	
	@Override
	public String toString() {
		return String.format("project: '%s'", getClass().getSimpleName(), m_columnSelection);
	}

	static class StreamImpl extends AbstractRecordStream {
		private final RecordStream m_input;
		private final ColumnSelector m_selector;
		private final Record m_output;
		
		StreamImpl(RecordStream input, ColumnSelector selector) {
			m_input = input;
			m_selector = selector;
			
			m_output = DefaultRecord.of(m_selector.getRecordSchema());
		}
		
		@Override
		public RecordSchema getRecordSchema() {
			return m_selector.getRecordSchema();
		}

		@Override
		protected void closeInGuard() throws Exception {
			m_input.close();
		}
		
		@Override
		public Record next() {
			Record record;
			if ( (record = m_input.next()) != null ) {
				m_selector.select(record, m_output);
				return m_output;
			}
			else {
				return null;
			}
		}
		
		@Override
		public Record nextCopy() {
			Record record;
			if ( (record = m_input.next()) != null ) {
				Record output = DefaultRecord.of(getRecordSchema());
				m_selector.select(record, output);
				return output;
			}
			else {
				return null;
			}
		}
	}
}