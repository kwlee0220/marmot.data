package marmot.dataset;

import com.vividsolutions.jts.geom.Envelope;

import marmot.RecordReader;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.RecordWriter;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface DataSet extends RecordReader, RecordWriter {
	/**
	 * 본 데이터세트의 식별자를 반환한다.
	 * 
	 * @return	데이터세트 식별자
	 */
	public String getId();
	
	/**
	 * 본 데이터세트의 등록정보 객체를 반환한다.
	 * 
	 * @return	데이터세트 등록정보 객체
	 */
	public DataSetInfo getDataSetInfo();
	
	/**
	 * 본 데이터세트의 레코드 스키마를 반환한다.
	 * 
	 * @return	레코드 스키마
	 */
	public RecordSchema getRecordSchema();
	
	/**
	 * 본 데이터세트에 포함된 모든 레코드들의 최소 사각 영역(MBR)을 반환한다.
	 * 
	 * @return	최소 사각 영역
	 */
	public Envelope getBounds();
	
	/**
	 * 본 데이터세트에 포함된 모든 레코드 수를 반환한다.
	 * 
	 * @return	레코드 수
	 */
	public long getRecordCount();
	
	/**
	 * 본 데이터세트의 크기를 반환한다.
	 * 
	 * @return	데이터세트 크기
	 */
	public long getLength();

	/**
	 * 데이타세트에 포함된 레코드의 스트림을 반환한다.
	 * 
	 * @return	레코드 스트림
	 */
	@Override
	public RecordStream read();

	/**
	 * 주어진 레코드 스트림에 포함된 레코드를 본 데이타세트에 저장한다.
	 * 
	 * @param stream	레코드 스트림
	 * @return	저장된 레코드 갯수
	 */
	@Override
	public long write(RecordStream stream);

	/**
	 * 주어진 레코드 스트림에 포함된 레코드를 본 데이타세트에 추가한다.
	 * 
	 * @param stream	레코드 스트림
	 */
	public void append(RecordStream stream);
}
