package marmot.dataset;

import com.vividsolutions.jts.geom.Envelope;

import marmot.RecordSchema;
import marmot.RecordStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class AbstractDataSet<T extends DataSetServer> implements DataSet {
	protected final T m_server;
	protected DataSetInfo m_info;	// dataset write/append에 따라 변경될 수 있음
	
	protected AbstractDataSet(T server, DataSetInfo info) {
		m_server = server;
		m_info = info;
	}
	
	public T getDataSetServer() {
		return m_server;
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
	public void append(RecordStream stream) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public String toString() {
		return String.format("%s: id=%s", getClass().getSimpleName(), getId());
	}
}
