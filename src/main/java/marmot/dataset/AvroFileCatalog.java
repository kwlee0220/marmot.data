package marmot.dataset;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import utils.KeyValue;
import utils.func.FOption;
import utils.stream.FStream;
import utils.stream.KeyValueFStream;

import marmot.Record;
import marmot.RecordReader;
import marmot.RecordStream;
import marmot.RecordWriter;
import marmot.avro.AvroFileRecordReader;
import marmot.avro.AvroFileRecordWriter;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AvroFileCatalog implements Catalog {
	
	private final File m_catalogFile;
	private Map<String,DataSetInfo> m_byIdMap = Maps.newHashMap();
	private ListMultimap<String,DataSetInfo> m_byFolderMap = ArrayListMultimap.create();
	
	public AvroFileCatalog(File catalogFile) {
		m_catalogFile = catalogFile;
		
		RecordReader reader = new AvroFileRecordReader(m_catalogFile);
		try ( RecordStream strm = reader.read() ) {
			for ( Record record = strm.next(); record != null; record = strm.next() ) {
				DataSetInfo dsInfo = DataSetInfo.fromRecord(record);
				
				String id = Catalogs.normalize(dsInfo.getId());
				m_byIdMap.put(id, dsInfo);
				m_byFolderMap.put(getParentId(id), dsInfo);
			}
		}
	}
	
	public File getCatalogFile() {
		return m_catalogFile;
	}

	@Override
	public FOption<DataSetInfo> getDataSetInfo(String dsId) {
		dsId = Catalogs.normalize(dsId);
		return FOption.ofNullable(m_byIdMap.get(dsId)).map(DataSetInfo::duplicate);
	}

	@Override
	public List<DataSetInfo> getDataSetInfoAll() {
		return KeyValueFStream.from(m_byFolderMap.asMap())
								.flatMap(kv -> FStream.from(kv.value()))
								.map(DataSetInfo::duplicate)
								.toList();
	}

	@Override
	public List<DataSetInfo> getDataSetInfoAllInDir(String keyFolder, boolean recursive) {
		String prefix = Catalogs.normalize(keyFolder);
		KeyValueFStream<String, Collection<DataSetInfo>> strm = KeyValueFStream.from(m_byFolderMap.asMap());
		strm = ( recursive )
				? strm.filterKey(folder -> folder.startsWith(prefix))
				: strm.filterKey(folder -> folder.equals(prefix));
		return strm.values()
					.flatMap(infos -> FStream.from(infos))
					.toList();
	}

	@Override
	public boolean isDirectory(String id) {
		String prefix = Catalogs.normalize(id);
		return KeyValueFStream.from(m_byFolderMap.asMap())
								.filterKey(folder -> folder.startsWith(prefix))
								.exists();
	}

	@Override
	public DataSetInfo updateDataSetInfo(DataSetInfo info) {
		String id = Catalogs.normalize(info.getId());
		DataSetInfo found = m_byIdMap.get(id);
		if ( found == null ) {
			throw new DataSetNotFoundException("dataset=" + id);
		}
		
		found.setRecordCount(info.getRecordCount());
		found.setBounds(info.getBounds());
		found.setParameter(info.getParameter());
		found.setUpdateEpochMillis(info.getUpdateEpochMillis());
		save();
		
		return found.duplicate();
	}

	@Override
	public DataSetInfo insertDataSetInfo(DataSetInfo info) {
		String id = Catalogs.normalize(info.getId());
		DataSetInfo found = m_byIdMap.get(id);
		if ( found != null ) {
			throw new DataSetExistsException("dataset id=" + id);
		}
		
		DataSetInfo copied = info.duplicate();
		insert(id, copied);
		save();
		
		return copied.duplicate();
	}

	@Override
	public DataSetInfo insertOrReplaceDataSetInfo(DataSetInfo info) {
		String id = Catalogs.normalize(info.getId());
		DataSetInfo found = m_byIdMap.get(id);
		if ( found != null ) {
			found.setRecordCount(info.getRecordCount());
			found.setBounds(info.getBounds());
			found.setUpdateEpochMillis(info.getUpdateEpochMillis());
			save();
			
			return found.duplicate();
		}
		else {
			DataSetInfo copied = info.duplicate();
			m_byIdMap.put(id, copied);
			
			String folder = getParentId(id);
			m_byFolderMap.put(folder, copied);
			save();

			return copied.duplicate();
		}
	}

	@Override
	public boolean deleteDataSetInfo(String id) {
		id = Catalogs.normalize(id);
		
		DataSetInfo found = delete(id);
		if ( found == null ) {
			return false;
		}
		
		save();
		
		return true;
	}

	@Override
	public DataSetInfo moveDataSetInfo(String id, String newId) {
		id = Catalogs.normalize(id);
		newId = Catalogs.normalize(newId);
		if ( isDirectory(newId) ) {
			String name = Catalogs.getName(id);
			newId = Catalogs.toDataSetId(newId, name);
		}
		DataSetInfo found = m_byIdMap.get(newId);
		if ( found != null ) {
			throw new DataSetExistsException("dest dataset id=" + newId);
		}
		
		found = delete(id);
		if ( found == null ) {
			throw new DataSetNotFoundException("src dataset id=" + id);
		}
		
		DataSetInfo moved = new DataSetInfo(newId, found.getType(), found.getRecordSchema());
		moved.setBounds(found.getBounds());
		moved.setRecordCount(found.getRecordCount());
		moved.setParameter(found.getParameter());
		moved.setUpdateEpochMillis(found.getUpdateEpochMillis());
		insert(newId, moved);
		
		save();
		
		return moved.duplicate();
	}

	@Override
	public Set<String> getDirAll() {
		return m_byFolderMap.keySet();
	}

	@Override
	public String getParentDir(String folder) {
		return null;
	}

	@Override
	public Set<String> getSubDirAll(String folder, boolean recursive) {
		String prefix = Catalogs.normalize(folder);
		KeyValueFStream<String, Collection<DataSetInfo>> strm = KeyValueFStream.from(m_byFolderMap.asMap());
		strm = ( recursive )
				? strm.filterKey(fdr -> fdr.startsWith(prefix))
				: strm.filterKey(fdr -> fdr.equals(prefix));
		return strm.keys().toSet();
	}

	@Override
	public int deleteDir(String folder) {
		String prefix = Catalogs.normalize(folder);

		Multimap<String,DataSetInfo> old = m_byFolderMap;
		m_byFolderMap = ArrayListMultimap.create();
		m_byIdMap = KeyValueFStream.from(m_byIdMap)
									.filterKey(id -> !id.startsWith(prefix))
									.peek(kv -> m_byFolderMap.put(getParentId(kv.key()), kv.value()))
									.toKeyValueStream(KeyValue::key, KeyValue::value)
									.toMap();	
		int count = old.size() - m_byFolderMap.size();
		save();
		
		return count;
	}

	@Override
	public void moveDir(String folder, String newFolder) {
		folder = Catalogs.normalize(folder);
		String newPrefix = Catalogs.normalize(newFolder);
		if ( !folder.endsWith("/") ) {
			folder = folder + "/";
		}
		String prefix = folder;
		int prefixLen = prefix.length();

		m_byFolderMap = ArrayListMultimap.create();
		m_byIdMap = KeyValueFStream.from(m_byIdMap)
									.map(kv -> {
										if ( kv.key().startsWith(prefix) ) {
											String suffix = kv.key().substring(prefixLen);
											String newId = newPrefix + Catalogs.ID_DELIM + suffix;
											return kv.value().clone(newId);
										}
										else {
											return kv.value();
										}
									})
									.peek(info -> m_byFolderMap.put(getParentId(info.getId()), info))
									.tagKey(info -> info.getId())
									.toMap();
		save();
	}
	
	public static void createCatalog(File catalogFile) {
		AvroFileRecordWriter writer = new AvroFileRecordWriter(catalogFile, DataSetInfo.SCHEMA);
		writer.write(RecordStream.empty(DataSetInfo.SCHEMA));
	}
	
	public static void dropCatalog(File catalogFile) {
		FileUtils.deleteQuietly(catalogFile);
	}
	
	private void save() {
		RecordWriter writer = new AvroFileRecordWriter(m_catalogFile, DataSetInfo.SCHEMA);
		RecordStream strm = RecordStream.from(DataSetInfo.SCHEMA, FStream.from(m_byIdMap.values())
																		.map(DataSetInfo::toRecord));
		writer.write(strm);
	}
	
	private static String getParentId(String id) {
		id = Catalogs.normalize(id);
		return Catalogs.getFolder(id);
	}

	private void insert(String id, DataSetInfo copied) {
		m_byIdMap.put(id, copied);
		String folder = getParentId(id);
		m_byFolderMap.put(folder, copied);
	}

	private DataSetInfo delete(String id) {
		DataSetInfo found = m_byIdMap.remove(id);
		if ( found == null ) {
			return null;
		}
		else {
			String folder = getParentId(id);
			m_byFolderMap.remove(folder, found);
			
			return found;
		}
	}
}
