package marmot.dataset;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;

import marmot.RecordStream;
import marmot.avro.AvroFileRecordWriter;
import marmot.avro.AvroUtils;
import marmot.avro.MultiFileAvroReader;
import utils.func.Try;
import utils.io.IOUtils;
import utils.jdbc.JdbcProcessor;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LfsAvroDataSetServer extends AbstractDataSetServer {
	private final File m_root;
	
	public LfsAvroDataSetServer(JdbcProcessor jdbc, File root) {
		super(new Catalog(jdbc));
		
		m_root = root;
	}
	
	public JdbcProcessor getJdbcProcessor() {
		return getCatalog().getJdbcProcessor();
	}
	
	public File getRoot() {
		return m_root;
	}
	
	@Override
	public String getDataSetUri(String dsId) {
		dsId = Catalogs.normalize(dsId);
		return new File(m_root, dsId.substring(1)).toURI().toString();
	}
	
	public static LfsAvroDataSetServer format(JdbcProcessor jdbc, File root) throws IOException {
		drop(jdbc, root);
		return create(jdbc, root);
	}
	
	public static LfsAvroDataSetServer create(JdbcProcessor jdbc, File root) throws IOException {
		Catalog.createCatalog(jdbc);
		FileUtils.forceMkdir(root);

		return new LfsAvroDataSetServer(jdbc, root);
	}
	
	public static void drop(JdbcProcessor jdbc, File root) {
		Catalog.dropCatalog(jdbc);
		Try.run(() -> FileUtils.deleteDirectory(root));
	}
	
	@Override
	public DataSet createDataSet(DataSetInfo dsInfo, boolean force) throws DataSetExistsException {
		LfsDataSet ds = (LfsDataSet)super.createDataSet(dsInfo, force);
		File file = ds.getFile();
		
		if ( force && file.exists() ) {
			try {
				FileUtils.forceDelete(file);
				FileUtils.forceMkdir(file);
			}
			catch ( IOException e ) {
				throw new DataSetException("fails to clean up files: " + file, e);
			}
		}
		
		Schema avroSchema = AvroUtils.toSchema(dsInfo.getRecordSchema());
		File schemaFile = new File(file, "_schema.avsc");
		PrintWriter pw = null;
		try {
			FileUtils.forceMkdirParent(schemaFile);
			pw = new PrintWriter(new FileWriter(schemaFile));
			pw.println(avroSchema.toString(true));
			
			return ds;
		}
		catch ( IOException e ) {
			throw new DataSetException("fails to write avro schema file: " + schemaFile, e);
		}
		finally {
			IOUtils.closeQuietly(pw);
		}
	}

	@Override
	public boolean deleteDataSet(String id) {
		boolean done = super.deleteDataSet(id);
		return FileUtils.deleteQuietly(getFile(id)) || done;
	}

	@Override
	public void deleteDir(String folder) {
		super.deleteDir(folder);
		FileUtils.deleteQuietly(getFile(folder));
	}

	@Override
	public DataSet moveDataSet(String id, String newId) {
		Catalog catalog = getCatalog();
		
		DataSetInfo oldInfo = catalog.getDataSetInfo(id).getOrThrow(() -> new DataSetNotFoundException(id));
		DataSetInfo newInfo = catalog.moveDataSetInfo(id, newId);
		
		try {
			File srcDir = getFile(oldInfo.getId());
			File destDir = getFile(newInfo.getId());
			FileUtils.moveDirectory(srcDir, destDir);
			
			return toDataSet(newInfo);
		}
		catch ( IOException e ) {
			catalog.moveDataSetInfo(newId, id);
			
			throw new DataSetException(String.format("fails to move dataset: %s -> %s", id, newId), e);
		}
	}
	
	private File getFile(String id) {
		if ( id.startsWith("/") ) {
			id = id.substring(1);
		}
		
		return new File(m_root, id);
	}

	@Override
	protected DataSet toDataSet(DataSetInfo info) {
		return new LfsDataSet(this, info, getFile(info.getId()));
	}

	public static final class LfsDataSet extends AbstractDataSet<LfsAvroDataSetServer> {
		private final File m_start;
		
		public LfsDataSet(LfsAvroDataSetServer server, DataSetInfo info, File start) {
			super(server, info);
			
			m_start = start;
		}
		
		public File getFile() {
			return m_start;
		}

		@Override
		public RecordStream read() {
			return MultiFileAvroReader.scan(m_start).read();
		}

		@Override
		public long write(RecordStream stream) {
			File partFile = new File(m_start, UUID.randomUUID().toString() + ".avro");
			return new AvroFileRecordWriter(partFile).write(stream);
		}
	
		@Override
		public long getLength() {
			return getFile().length();
		}
	}
}
