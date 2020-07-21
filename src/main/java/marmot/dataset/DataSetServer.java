package marmot.dataset;

import java.util.List;

import marmot.RecordStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface DataSetServer {
	/**
	 * 주어진 식별자에 해당하는 데이터세트({@link DataSet}) 객체를 반환한다.
	 * 
	 * @param id	데이터세트 식별자
	 * @return	DataSet 객체
	 * @throws DataSetNotFoundException	식별자에 해당하는 데이터세트가 없는 경우.
	 */
	public DataSet getDataSet(String dsId);
	
	/**
	 * 주어진 식별자에 해당하는 데이터세트({@link DataSet}) 객체를 반환한다.
	 * 식별자에 해당하는 데이터세트가 없는 경우는 {@code null}을 반환한다.
	 * 
	 * @param id	데이터세트 식별자
	 * @return	DataSet 객체
	 */
	public DataSet getDataSetOrNull(String dsId);
	
	public DataSet updateDataSet(DataSetInfo info);
	
	/**
	 * 시스템에 등록된 모든 데이터세트를 반환한다.
	 * 
	 * @return	데이터세트 리스트.
	 */
	public List<DataSet> getDataSetAll();
	
	public DataSet createDataSet(DataSetInfo dsInfo, boolean force) throws DataSetExistsException;
	public default DataSet createDataSet(String dsId, RecordStream stream, boolean force) throws DataSetExistsException {
		DataSetInfo info = new DataSetInfo(dsId, stream.getRecordSchema());
		DataSet ds = createDataSet(info, force);
		ds.write(stream);
		
		return ds;
	}
	
	/**
	 * 주어진 식별자에 해당하는 데이터세트를 삭제시킨다.
	 * 
	 * @param id	대상 데이터세트 식별자.
	 * @return	 데이터세트 삭제 여부.
	 */
	public boolean deleteDataSet(String id);
	
	/**
	 * 데이터세트의 이름을 변경시킨다.
	 * 
	 * @param id 	변경시킬 데이터세트 식별자.
	 * @param newId 변경될 데이터세트 식별자.
	 * @return	변경된 이름의 데이터세트
	 */
	public DataSet moveDataSet(String id, String newId);
	
	/**
	 * 주어진 이름의 폴더에 저장된 모든 데이터세트를 반환한다.
	 * <p>
	 * 폴더는 계층구조를 갖고 있기 때문에 {@code recursive} 인자에 따라
	 * 지정된 폴더에 저장된 데이터세트를 반환할 수도 있고, 해당 폴더와
	 * 모든 하위 폴더에 저장된 데이터세트들을 반환하게 할 수 있다. 
	 * 
	 * @param folder	대상 폴더 이름.
	 * @param recursive	하위 폴더 포함 여부.
	 * @return	데이터세트 설정정보 리스트.
	 */
	public List<DataSet> getDataSetAllInDir(String folder, boolean recursive);
	
	/**
	 * 시스템에 등록된 모든 폴더의 이름들을 반환한다.
	 * 
	 * @return	폴더 이름 리스트.
	 */
	public List<String> getDirAll();
	
	/**
	 * 주어진 이름의 폴더에 등록된 모든 하위 폴더 이름을 반환한다.
	 * <p>
	 * 폴더는 계층구조를 갖고 있기 때문에 {@code recursive} 인자에 따라
	 * 지정된 폴더에 바로 속한 하위 폴더의 이름들만 반환할 수도 있고, 해당 폴더의
	 * 모든 하위 폴더의 이름을 반환하게 할 수 있다. 
	 * 
	 * @param folder	대상 폴더 이름.
	 * @param recursive	하위 폴더 포함 여부.
	 * @return	폴더 이름 리스트.
	 */
	public List<String> getSubDirAll(String folder, boolean recursive);
	
	/**
	 * 주어진 이름의 폴더의 상위 폴더 이름을 반환한다.
	 * 
	 * @param folder	대상 폴더 이름.
	 * @return	폴더 이름.
	 */
	public String getParentDir(String folder);
	
	/**
	 * 주어진 이름의 폴더의 이름을 변경시킨다.
	 * 
	 * @param path		변경시킬 대상 폴더 이름.
	 * @param newPath	변경된 새 폴더 이름.
	 */
	public void moveDir(String path, String newPath);
	
	/**
	 * 주어진 이름의 폴더 및 모든 하위 폴더들과 각 폴더에 등록된 모든 데이터세트들을 제거한다.
	 * 
	 * @param folder	대상 폴더 이름.
	 */
	public void deleteDir(String folder);
}
