package marmot;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface RecordWriter {
	/**
	 * 레코드 세트에 포함된 모든 레코드를 저장한다.
	 * 
	 * @param dataset	저장할 데이터 세트 객체.
	 * @return	저장된 레코드의 갯수.
	 */
	public long write(RecordStream stream);
}
