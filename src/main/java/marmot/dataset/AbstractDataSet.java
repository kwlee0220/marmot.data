package marmot.dataset;

import org.locationtech.jts.geom.Envelope;

import marmot.RecordSchema;
import marmot.RecordStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class AbstractDataSet implements DataSet {
//	protected final T m_server;
	protected DataSetInfo m_info;	// dataset write/append에 따라 변경될 수 있음
	
	protected AbstractDataSet(DataSetInfo info) {
//		m_server = server;
		m_info = info;
	}

	@Override
	public DataSetInfo getDataSetInfo() {
		return m_info;
	}

	@Override
	public String getId() {
		return m_info.getId();
	}

	@Override
	public DataSetType getType() {
		return m_info.getType();
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_info.getRecordSchema();
	}

	@Override
	public Envelope getBounds() {
		return m_info.getBounds();
	}

	@Override
	public long getRecordCount() {
		return m_info.getRecordCount();
	}

	@Override
	public String getParameter() {
		return m_info.getParameter();
	}

	@Override
	public void append(RecordStream stream) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public String toString() {
		return String.format("%s: id=%s", getClass().getSimpleName(), getId());
	}
}
