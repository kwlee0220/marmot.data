package marmot.dataset;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;

import marmot.RecordSchema;
import utils.Utilities;
import utils.func.CheckedFunctionX;
import utils.func.FOption;
import utils.func.Try;
import utils.jdbc.JdbcException;
import utils.jdbc.JdbcProcessor;
import utils.jdbc.JdbcUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class JdbcCatalog implements Catalog {
	private final JdbcProcessor m_jdbc;
	
	public JdbcCatalog(JdbcProcessor jdbc) {
		Utilities.checkNotNullArgument(jdbc);
		
		m_jdbc = jdbc;
	}
	
	public JdbcProcessor getJdbcProcessor() {
		return m_jdbc;
	}
	
	public String toFilePath(String id) {
		return id;
	}

	/**
	 * 주어진 이름에 해당하는 데이터세트 정보를 반환한다.
	 * 
	 * @param dsId	데이터세트 이름
	 * @return	데이터세트 정보 객체.
	 * 			이름에 해당하는 데이터세트가 등록되지 않은 경우는
	 * 			{@code Option.none()}을 반환함.
	 * @throws CatalogException			카타로그 정보 접근 중 오류가 발생된 경우.
	 */
	@Override
	public FOption<DataSetInfo> getDataSetInfo(String dsId) {
		dsId = Catalogs.normalize(dsId);
		
		try ( Connection conn = m_jdbc.connect() ) {
			return getDataSetInfoInGuard(conn, dsId);
		}
		catch ( SQLException e ) {
			throw new CatalogException(e);
		}
	}

	@Override
	public List<DataSetInfo> getDataSetInfoAll() {
		try ( Connection conn = m_jdbc.connect();
			PreparedStatement pstmt = conn.prepareStatement(SQL_GET_DATASET_ALL); ) {
			return JdbcUtils.stream(pstmt.executeQuery(), s_toDsInfo)
							.collect(Collectors.toList());
		}
		catch ( SQLException e ) {
			throw new JdbcException(e);
		}
	}

	@Override
	public DataSetInfo updateDataSetInfo(DataSetInfo info) {
		Utilities.checkNotNullArgument(info, "DataSetInfo should not be null.");

		String id = Catalogs.normalize(info.getId());
		try ( Connection conn = m_jdbc.connect();
			PreparedStatement pstmt = conn.prepareStatement(SQL_UPDATE_DATASET) ) {			
			pstmt.setLong(1, info.getRecordCount());
			pstmt.setString(2, fromEnvelope(info.getBounds()));
			pstmt.setString(3, info.getParameter());
			pstmt.setLong(4, System.currentTimeMillis());
			pstmt.setString(5, id);
			
			if ( pstmt.executeUpdate() <= 0 ) {
				throw new CatalogException("fails to update DataSet");
			}
			
			return getDataSetInfoInGuard(conn, id).get();
		}
		catch ( SQLException e ) {
			throw new CatalogException(e);
		}
	}

	@Override
	public DataSetInfo insertDataSetInfo(DataSetInfo info) {
		Utilities.checkNotNullArgument(info, "DataSetInfo should not be null.");

		String id = Catalogs.normalize(info.getId());
		try ( Connection conn = m_jdbc.connect() ) {
			// 주어진 경로명과 동일하거나, 앞은 동일하면서 더 긴 경로명의 데이터세트가 있는지
			// 조사한다.
			if ( existsDataSetInfoInGuard(conn, id) ) {
				throw new DataSetExistsException(id);
			}
			if ( existsDirectoryInGuard(conn, id) ) {
				throw new DataSetExistsException("directory=" + id);
			}
			
			insertDataSetInfoInGuard(conn, info);
			
			return getDataSetInfoInGuard(conn, id).get();
		}
		catch ( SQLException e ) {
			String state = e.getSQLState();
			if ( state.equals("23505") ) {
				throw new DataSetExistsException(id, e);
			}
			
			throw new CatalogException(e);
		}
	}

	@Override
	public DataSetInfo insertOrReplaceDataSetInfo(DataSetInfo info) {
		Utilities.checkNotNullArgument(info, "DataSetInfo should not be null.");

		String id = Catalogs.normalize(info.getId());
		try ( Connection conn = m_jdbc.connect() ) {
			// 주어진 경로명과 동일하거나, 앞은 동일하면서 더 긴 경로명의 데이터세트가 있는지
			// 조사한다.
			if ( existsDataSetInfoInGuard(conn, id) ) {
				try ( PreparedStatement pstmt = conn.prepareStatement(SQL_UPDATE_DATASET) ) {			
					pstmt.setLong(1, info.getRecordCount());
					pstmt.setString(2, fromEnvelope(info.getBounds()));
					pstmt.setString(3, info.getParameter());
					pstmt.setLong(4, System.currentTimeMillis());
					pstmt.setString(5, id);
					
					if ( pstmt.executeUpdate() <= 0 ) {
						throw new CatalogException("fails to update DataSet");
					}
				}
			}
			else {
				insertDataSetInfoInGuard(conn, info);
			}
			
			return getDataSetInfoInGuard(conn, id).get();
		}
		catch ( SQLException e ) {
			throw new CatalogException(e);
		}
	}

	@Override
	public DataSetInfo moveDataSetInfo(String id, String newId) {
		try ( Connection conn = m_jdbc.connect(); ) {
			return moveDataSetInGuard(conn, id, newId);
		}
		catch ( SQLException e ) {
			throw new CatalogException(e);
		}
	}

	@Override
	public boolean isDirectory(String id) {
		id = Catalogs.normalize(id);

		try ( Connection conn = m_jdbc.connect() ) {
			return existsDirectoryInGuard(conn, id);
		}
		catch ( SQLException e ) {
			throw new JdbcException(e);
		}
	}

	@Override
	public List<DataSetInfo> getDataSetInfoAllInDir(String folder, boolean recursive) {
		folder = Catalogs.normalize(folder);

		try ( Connection conn = m_jdbc.connect() ) {
			PreparedStatement pstmt;
			if ( recursive ) {
				pstmt = conn.prepareStatement(SQL_LIST_DATASETS_AT_FOLDER_RECURSIVE);
				pstmt.setString(1, folder);
				pstmt.setString(2, folder + "/%");
			}
			else {
				pstmt = conn.prepareStatement(SQL_LIST_DATASETS_AT_FOLDER);
				pstmt.setString(1, folder);
			}
			
			return JdbcUtils.stream(pstmt.executeQuery(), s_toDsInfo)
							.collect(Collectors.toList());
		}
		catch ( SQLException e ) {
			throw new JdbcException(e);
		}
	}

	@Override
	public boolean deleteDataSetInfo(String id) {
		id = Catalogs.normalize(id);

		try ( Connection conn = m_jdbc.connect(); ) {
			return deleteDataSetInfoInGuard(conn, id);
		}
		catch ( SQLException e ) {
			throw new CatalogException(e);
		}
	}

	@Override
	public Set<String> getDirAll() {
		try ( Connection conn = m_jdbc.connect();
			PreparedStatement pstmt = conn.prepareStatement(SQL_GET_FOLDER_ALL); ) {
			return JdbcUtils.stream(pstmt.executeQuery(), s_toFolder)
							.collect(Collectors.toSet());
		}
		catch ( SQLException e ) {
			throw new JdbcException(e);
		}
	}

	@Override
	public String getParentDir(String folder) {
		folder = Catalogs.normalize(folder);
		return Catalogs.getFolder(folder);
	}

	@Override
	public Set<String> getSubDirAll(String folder, boolean recursive) {
		String prefix = Catalogs.normalize(folder);
		if ( !prefix.endsWith("/") ) {
			prefix = prefix + "/";
		}
		int prefixLen = prefix.length();

		try ( Connection conn = m_jdbc.connect();
			PreparedStatement pstmt = conn.prepareStatement(SQL_GET_SUBFOLDER_ALL); ) {
			pstmt.setString(1, prefix);
			pstmt.setString(2, prefix + "%");
			
			Stream<String> folderStrm = JdbcUtils.stream(pstmt.executeQuery(), s_toFolder);
			if ( !recursive ) {
				folderStrm = folderStrm.map(name -> name.substring(prefixLen))
										.map(name -> {
											int idx = name.indexOf(Catalogs.ID_DELIM);
											if ( idx >= 0 ) {
												name = name.substring(0, idx);
											}
											return name;
										})
										.distinct();
			}
			return folderStrm.collect(Collectors.toSet());
		}
		catch ( SQLException e ) {
			throw new JdbcException(e);
		}
	}

	@Override
	public int deleteDir(String folder) {
		folder = Catalogs.normalize(folder);

		try ( Connection conn = m_jdbc.connect();
			PreparedStatement pstmt = conn.prepareStatement(SQL_DELETE_FOLDER); ) {
			pstmt.setString(1, folder);
			pstmt.setString(2, folder + "/%");
			
			return pstmt.executeUpdate();
		}
		catch ( SQLException e ) {
			throw new CatalogException(e);
		}
	}

	@Override
	public void moveDir(String path, String newPath) {
		String prefix = Catalogs.normalize(path);
		if ( !prefix.endsWith("/") ) {
			prefix = prefix + "/";
		}
		int prefixLen = prefix.length();
		
		newPath = Catalogs.normalize(newPath);
		List<DataSetInfo> infoList = getDataSetInfoAllInDir(path, true);
		try ( Connection conn = m_jdbc.connect(); ) {
			for ( DataSetInfo info: infoList ) {
				String suffix = info.getId().substring(prefixLen);
				String newId = newPath + Catalogs.ID_DELIM + suffix;
				
				moveDataSetInGuard(conn, info.getId(), newId);
			}
		}
		catch ( SQLException e ) {
			throw new CatalogException(e);
		}
	}
	
	private boolean existsDirectoryInGuard(Connection conn, String id) throws SQLException {
		try ( PreparedStatement pstmt = conn.prepareStatement(SQL_IS_FOLDER) ) {
			pstmt.setString(1, id);
	
			return JdbcUtils.stream(pstmt.executeQuery(), s_toCount).findAny().get() > 0;
		}
	}
	
	private boolean existsDataSetInfoInGuard(Connection conn, String id) throws SQLException {
		try ( PreparedStatement pstmt = conn.prepareStatement(SQL_EXISTS_DATASET) ) {
			pstmt.setString(1, id);
			pstmt.setString(2, id + "/%");
			try ( ResultSet rs = pstmt.executeQuery(); ) {
				return rs.next();
			}
		}
	}
	
	private FOption<DataSetInfo> getDataSetInfoInGuard(Connection conn, String dsId) {
		try ( PreparedStatement pstmt = conn.prepareStatement(SQL_GET_DATASET); ) {
			pstmt.setString(1, dsId);
			
			return FOption.from(JdbcUtils.stream(pstmt.executeQuery(), s_toDsInfo)
												.findAny());
		}
		catch ( SQLException e ) {
			throw new CatalogException(e);
		}
	}
	
	private void insertDataSetInfoInGuard(Connection conn, DataSetInfo info) throws SQLException {
		String id = Catalogs.normalize(info.getId());
		try ( PreparedStatement pstmt = conn.prepareStatement(SQL_INSERT_DATASET) ) {
			pstmt.setString(1, id);
			pstmt.setString(2, getParentDir(id));
			pstmt.setString(3, info.getType().name());
			pstmt.setString(4, info.getRecordSchema().toTypeId());
			pstmt.setLong(5, info.getRecordCount());
			pstmt.setString(6, fromEnvelope(info.getBounds()));
			pstmt.setString(7, info.getParameter());
			pstmt.setLong(8, System.currentTimeMillis());
			
			if ( pstmt.executeUpdate() <= 0 ) {
				throw new CatalogException("fails to insert a DataSet");
			}
		}
	}
	
	private boolean deleteDataSetInfoInGuard(Connection conn, String id) throws SQLException {
		try ( PreparedStatement pstmt = conn.prepareStatement(SQL_DELETE_DATASET); ) {
			pstmt.setString(1, id);
			return pstmt.executeUpdate() > 0;
		}
	}

	private DataSetInfo moveDataSetInGuard(Connection conn, String id, String newId) throws SQLException {
		DataSetInfo info = getDataSetInfoInGuard(conn, id)
								.getOrThrow(()->new DataSetNotFoundException(id));
		
		if ( existsDataSetInfoInGuard(conn, newId) ) {
			throw new DataSetExistsException("target dataset exists: id=" + newId);
		}
		if ( existsDirectoryInGuard(conn, newId) ) {
			String name = Catalogs.getName(id);
			newId = Catalogs.toDataSetId(newId, name);
		}
		deleteDataSetInfoInGuard(conn, info.getId());
		
		return info.clone(newId);
	}
	
	private static final String SQL_EXISTS_DATASET
		= "select id from datasets where id = ? or id like ?";
	
	private static final String DATASET_COLUMNS
		= "id, folder, type, schema, count, bounds, parameter, updated_millis ";
	private static final String SQL_GET_DATASET_ALL
		= "select " + DATASET_COLUMNS + "from datasets";

	private static final String SQL_GET_DATASET
		= "select " + DATASET_COLUMNS + "from datasets where id=?";
	
	private static final String SQL_IS_FOLDER
		= "select count(*) from datasets where folder=?";
	
	private static final String SQL_LIST_DATASETS_AT_FOLDER
		= "select " + DATASET_COLUMNS + "from datasets where folder=?";
	
	private static final String SQL_LIST_DATASETS_AT_FOLDER_RECURSIVE
		= "select " + DATASET_COLUMNS + "from datasets where folder = ? or folder LIKE ?";
	
	private static final String SQL_INSERT_DATASET
		= "insert into datasets (" + DATASET_COLUMNS + ") values (?,?,?,?,?,?,?,?)";
	
	private static final String SQL_UPDATE_DATASET
		= "update datasets set count = ?, bounds = ?, parameter = ?, updated_millis = ? where id=?";
	
	private static final String SQL_DELETE_DATASET = "delete from datasets where id = ?";
	
	private static final String SQL_GET_FOLDER_ALL
		= "select distinct(folder) from datasets where folder <> ''";
	
	private static final String SQL_GET_SUBFOLDER_ALL
		= "select distinct(folder) from datasets where folder <> ? and folder LIKE ?";
	
	private static final String SQL_DELETE_FOLDER = "delete from datasets "
												+ "where folder = ? or folder like ?";
	
	private static final String SQL_CREATE_DATASETS
		= "create table datasets ("
		+ 	"id varchar not null,"
		+ 	"folder varchar not null,"
		+ 	"type varchar not null,"
		+ 	"schema varchar not null,"
		+ 	"count bigint not null,"
		+ 	"bounds varchar,"
		+ 	"parameter varchar,"
		+ 	"updated_millis bigint not null,"
		+ 	"primary key (id)"
		+ ")";
	
	public static void createCatalog(JdbcProcessor jdbc) {
		try ( Connection conn = jdbc.connect() ) {
			Statement stmt = conn.createStatement();
			stmt.executeUpdate(SQL_CREATE_DATASETS);
		}
		catch ( SQLException e ) {
			throw new CatalogException(e);
		}
	}
	
	public static void dropCatalog(JdbcProcessor jdbc) {
		try ( Connection conn = jdbc.connect() ) {
			
			final Statement stmt = conn.createStatement();
			Try.run(()->stmt.executeUpdate("drop table datasets"));
		}
		catch ( SQLException e ) {
			throw new CatalogException(e);
		}
	}

	private static final CheckedFunctionX<ResultSet,DataSetInfo,SQLException> s_toDsInfo = rs -> {
		try {
			String id = rs.getString(1);
			DataSetType type = DataSetType.valueOf(rs.getString(3));
			RecordSchema schema = RecordSchema.fromTypeId(rs.getString(4));
			
			DataSetInfo dsInfo = new DataSetInfo(id, type, schema);
			dsInfo.setRecordCount(rs.getLong(5));
			dsInfo.setBounds(toEnvelope(rs.getString(6)));
			dsInfo.setParameter(rs.getString(7));
			dsInfo.setUpdateEpochMillis(rs.getLong(8));
			
			return dsInfo;
		}
		catch ( SQLException e ) {
			throw new CatalogException(e);
		}
	};
	
	private static final Envelope toEnvelope(String envlStr) {
		if ( envlStr != null ) {
			double[] v = Stream.of(envlStr.split(";"))
								.mapToDouble(Double::parseDouble)
								.toArray();
			return new Envelope(new Coordinate(v[0], v[1]), new Coordinate(v[2], v[3]));
		}
		else {
			return null;
		}
	}
	
	private static final String fromEnvelope(Envelope envl) {
		if ( envl != null && !envl.isNull() ) {
			return Stream.of(envl.getMinX(),envl.getMinY(), envl.getMaxX(),envl.getMaxY())
						.map(Object::toString)
						.collect(Collectors.joining(";"));
		}
		else {
			return null;
		}
	}
	
	private static final CheckedFunctionX<ResultSet,String,SQLException> s_toFolder = rs -> {
		try {
			return rs.getString(1);
		}
		catch ( SQLException e ) {
			throw new CatalogException(e);
		}
	};
	
	private static final CheckedFunctionX<ResultSet,Integer,SQLException> s_toCount = rs -> {
		try {
			return rs.getInt(1);
		}
		catch ( SQLException e ) {
			throw new CatalogException(e);
		}
	};
}
