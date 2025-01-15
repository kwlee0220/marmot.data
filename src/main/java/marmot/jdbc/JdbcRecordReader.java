package marmot.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

import utils.jdbc.JdbcProcessor;

import marmot.Column;
import marmot.DefaultRecord;
import marmot.Record;
import marmot.RecordReader;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.RecordStreamException;
import marmot.dataset.DataSetException;
import marmot.stream.AbstractRecordStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class JdbcRecordReader implements RecordReader {
	private final JdbcRecordAdaptor m_adaptor;
	private final String m_sql;
	
	public JdbcRecordReader(JdbcRecordAdaptor adaptor, String tblName) {
		m_adaptor = adaptor;
		m_sql = adaptor.getRecordSchema()
						.streamColumns()
						.map(Column::name)
						.join(", ", "select ", " from " + tblName);
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_adaptor.getRecordSchema();
	}

	@Override
	public RecordStream read() {
		try {
			JdbcProcessor jdbc = m_adaptor.getJdbcProcessor();
			return new StreamImpl(jdbc.executeQuery(m_sql, true));
		}
		catch ( SQLException e ) {
			throw new DataSetException("fails to execute query: " + m_sql + ", cause=" + e);
		}
	}

	private class StreamImpl extends AbstractRecordStream {
		private final ResultSet m_rs;
		private final Record m_record;
		
		StreamImpl(ResultSet rs) {
			m_rs = rs;
			m_record = DefaultRecord.of(m_adaptor.getRecordSchema());
		}

		@Override
		protected void closeInGuard() throws Exception {
			m_rs.close();
		}

		@Override
		public RecordSchema getRecordSchema() {
			return m_adaptor.getRecordSchema();
		}
		
		@Override
		public Record next() {
			try {
				if ( m_rs.next() ) {
					m_adaptor.loadRecord(m_rs, m_record);
					return m_record;
				}
				else {
					return null;
				}
			}
			catch ( SQLException e ) {
				throw new RecordStreamException("" + e);
			}
		}
	}
}
