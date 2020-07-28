package marmot.dataset;

import java.util.List;
import java.util.Set;

import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface Catalog {
	/**
	 * 주어진 이름에 해당하는 데이터세트 정보를 반환한다.
	 * 
	 * @param dsId	데이터세트 이름
	 * @return	데이터세트 정보 객체.
	 * 			이름에 해당하는 데이터세트가 등록되지 않은 경우는
	 * 			{@code Option.none()}을 반환함.
	 * @throws CatalogException			카타로그 정보 접근 중 오류가 발생된 경우.
	 */
	public FOption<DataSetInfo> getDataSetInfo(String dsId);
	
	public List<DataSetInfo> getDataSetInfoAll();
	
	public DataSetInfo updateDataSetInfo(DataSetInfo info);
	public boolean deleteDataSetInfo(String id);
	public DataSetInfo moveDataSetInfo(String id, String newId);
	
	public boolean isDirectory(String id);
	
	public List<DataSetInfo> getDataSetInfoAllInDir(String folder, boolean recursive);
	public DataSetInfo insertDataSetInfo(DataSetInfo info);
	public DataSetInfo insertOrReplaceDataSetInfo(DataSetInfo info);
	
	public Set<String> getDirAll();
	public String getParentDir(String folder);
	public Set<String> getSubDirAll(String folder, boolean recursive);
	public int deleteDir(String folder);
	public void moveDir(String path, String newPath);
}
