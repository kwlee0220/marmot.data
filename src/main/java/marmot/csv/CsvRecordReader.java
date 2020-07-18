package marmot.csv;

import java.io.File;
import java.io.IOException;
import java.util.List;

import marmot.Column;
import marmot.RecordReader;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.RecordStreamException;
import marmot.optor.geo.ToGeometryPointReader;
import utils.func.Tuple4;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CsvRecordReader implements RecordReader {
	private final File m_start;
	private final String m_glob;
	private final CsvParameters m_params;
	private RecordSchema m_schema;
	
	public static RecordReader from(File start, CsvParameters params) {
		return from(start, params, "**/*.csv");
	}
	
	public static RecordReader from(File start, CsvParameters params, String glob) {
		RecordReader reader = new CsvRecordReader(start, params, glob);
		if ( params.pointColumns().isPresent() ) {
			Tuple4<String,String,String,String> ptCols = params.pointColumns().get();
			reader = new ToGeometryPointReader(reader, ptCols._3, ptCols._4, ptCols._1, ptCols._2);
			
			List<String> cols = reader.getRecordSchema().streamColumns()
										.map(Column::name)
										.filter(col -> !(col.equals(ptCols._3) || col.equals(ptCols._4)
														|| col.equals(ptCols._1)))
										.toList();
			cols.add(0, ptCols._1);;
			reader = reader.project(cols);
		}
		
		return reader;
	}
	
	private CsvRecordReader(File start, CsvParameters params, String glob) {
		m_start = start;
		m_glob = glob;
		m_params = params;
	}

	@Override
	public RecordSchema getRecordSchema() {
		if ( m_schema == null ) {
			try {
				List<File> files = MultiFileCsvRecordStream.collectCsvFiles(m_start, m_params, m_glob);
				if ( files.size() == 0 ) {
					throw new IllegalArgumentException("not CSV files: start=" + m_start);
				}
				try ( RecordStream strm = CsvRecordStream.from(files.get(0), m_params) ) {
					m_schema = strm.getRecordSchema();
				}
			}
			catch ( IOException e ) {
				throw new RecordStreamException("fails to parse CSV, start=" + m_start, e);
			}
		}
		
		return m_schema;
	}

	@Override
	public RecordStream read() {
		return new MultiFileCsvRecordStream(m_start, m_params);
	}
	
	@Override
	public String toString() {
		return String.format("%s[start=%s]params[%s]", getClass().getSimpleName(), m_start, m_params);
	}
}
