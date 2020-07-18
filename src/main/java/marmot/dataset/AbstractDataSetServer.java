package marmot.dataset;

import java.util.List;

import utils.Utilities;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class AbstractDataSetServer implements DataSetServer {
	private final Catalog m_catalog;
	
	abstract protected DataSet toDataSet(DataSetInfo info);
	abstract public String getDataSetUri(String dsId);
	
	protected AbstractDataSetServer(Catalog catalog) {
		m_catalog = catalog;
	}
	
	public Catalog getCatalog() {
		return m_catalog;
	}
	
	@Override
	public DataSet createDataSet(DataSetInfo dsInfo, boolean force) throws DataSetExistsException {
		Utilities.checkNotNullArgument(dsInfo, "DataSetInfo is null");
		
		// 'force' 옵션이 있는 경우는 식별자에 해당하는 미리 삭제한다.
		// 주어진 식별자가 폴더인 경우는 폴더 전체를 삭제한다.
		if ( force ) {
			deleteDir(dsInfo.getId());
			deleteDataSet(dsInfo.getId());
		}
		
		// 데이터세트 관련 정보를 카다로그에 추가시킨다.
		dsInfo = m_catalog.insertDataSetInfo(dsInfo);
		
		return toDataSet(dsInfo);
	}

	@Override
	public boolean deleteDataSet(String id) {
		return m_catalog.deleteDataSetInfo(id);
	}

	@Override
	public DataSet getDataSet(String dsId) {
		return m_catalog.getDataSetInfo(dsId)
						.map(this::toDataSet)
						.getOrThrow(() -> new DataSetNotFoundException(dsId));
	}

	@Override
	public DataSet getDataSetOrNull(String dsId) {
		return m_catalog.getDataSetInfo(dsId)
						.map(this::toDataSet)
						.getOrNull();
	}

	@Override
	public DataSet updateDataSet(DataSetInfo info) {
		info = m_catalog.updateDataSetInfo(info);
		return toDataSet(info);
	}

	@Override
	public List<DataSet> getDataSetAll() {
		return FStream.from(m_catalog.getDataSetInfoAll())
						.map(this::toDataSet)
						.toList();
	}

	@Override
	public List<DataSet> getDataSetAllInDir(String folder, boolean recursive) {
		return FStream.from(m_catalog.getDataSetInfoAllInDir(folder, recursive))
									.map(this::toDataSet)
									.toList();
	}

	@Override
	public void moveDataSet(String id, String newId) {
		DataSetInfo info = m_catalog.getDataSetInfo(id)
									.getOrThrow(()->new DataSetNotFoundException(id));
		
		if ( m_catalog.getDataSetInfo(newId).isPresent() ) {
			throw new DataSetExistsException("target dataset exists: id=" + newId);
		}
		if ( m_catalog.isDirectory(newId) ) {
			String name = Catalogs.getName(id);
			newId = Catalogs.toDataSetId(newId, name);
		}
		m_catalog.deleteDataSetInfo(info.getId());
		
		DataSetInfo newInfo = new DataSetInfo(newId, info.getRecordSchema());
		newInfo.setBounds(info.getBounds());
		newInfo.setRecordCount(info.getRecordCount());
		m_catalog.insertDataSetInfo(newInfo);
	}

	@Override
	public List<String> getDirAll() {
		return m_catalog.getDirAll();
	}

	@Override
	public List<String> getSubDirAll(String folder, boolean recursive) {
		return m_catalog.getSubDirAll(folder, recursive);
	}

	@Override
	public String getParentDir(String folder) {
		return m_catalog.getParentDir(folder);
	}

	@Override
	public void moveDir(String path, String newPath) {
		String prefix = Catalogs.normalize(path);
		if ( !prefix.endsWith("/") ) {
			prefix = prefix + "/";
		}
		int prefixLen = prefix.length();
		
		newPath = Catalogs.normalize(newPath);
		for ( DataSetInfo info: m_catalog.getDataSetInfoAllInDir(path, true) ) {
			String suffix = info.getId().substring(prefixLen);
			String newId = newPath + Catalogs.ID_DELIM + suffix;
			
			moveDataSet(info.getId(), newId);
		}
	}

	@Override
	public void deleteDir(String folder) {
		m_catalog.deleteDir(folder);
	}
}
