package marmot.dataset;

import marmot.RecordReader;
import marmot.RecordStream;
import marmot.RecordWriter;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DataSetImpl implements DataSet {
	private final DataSetServer m_server;
	private DataSetInfo m_info;
	private final RecordReader m_reader;
	private final RecordWriter m_writer;
	
	public DataSetImpl(DataSetServer server, DataSetInfo info, RecordReader reader, RecordWriter writer) {
		m_server = server;
		m_info = info;
		m_reader = reader;
		m_writer = writer;
	}

	@Override
	public DataSetInfo getDataSetInfo() {
		return m_info;
	}

	@Override
	public RecordStream read() {
		return m_reader.read();
	}

	@Override
	public void write(RecordStream stream) {
		// 쓰여지는 레코드 스트림의 통계정보를 기록한다.
//		StatsCollectingRecordStream collector = stream.collectStats();
		m_writer.write(stream);
		
		// 기록된 통계정보를 카다로그에 반영신킨다.
//		m_info.setRecordCount(collector.getRecordCount());
//		m_info.setBounds(collector.getBounds());
//		m_info = m_server.updateDataSet(m_info).getDataSetInfo();
	}

	@Override
	public void append(RecordStream stream) {
		throw new UnsupportedOperationException();
	}
}
