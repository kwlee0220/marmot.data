package marmot.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

import marmot.Column;
import marmot.DataSet;
import marmot.DataSetException;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.RecordStreamException;
import marmot.stream.AbstractRecordStream;
import utils.jdbc.JdbcProcessor;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class JdbcDataSet implements DataSet {
	private final JdbcRecordAdaptor m_adaptor;
	private final String m_sql;
	
	public JdbcDataSet(JdbcRecordAdaptor adaptor, String tblName) {
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
			return new StreamImpl(jdbc.executeQuery(m_sql));
		}
		catch ( SQLException e ) {
			throw new DataSetException("fails to execute query: " + m_sql + ", cause=" + e);
		}
	}

	private class StreamImpl extends AbstractRecordStream {
		private final ResultSet m_rs;
		
		StreamImpl(ResultSet rs) {
			m_rs = rs;
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
		public boolean next(Record output) {
			try {
				if ( m_rs.next() ) {
					m_adaptor.loadRecord(m_rs, output);
					return true;
				}
				else {
					return false;
				}
			}
			catch ( SQLException e ) {
				throw new RecordStreamException("" + e);
			}
		}
	}
}
