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
		try ( PrintWriter pw = new PrintWriter(new FileWriter(schemaFile)) ) {
			pw.println(avroSchema.toString(true));
			
			return ds;
		}
		catch ( IOException e ) {
			throw new DataSetException("fails to write avro schema file: " + schemaFile, e);
		}
	}

	@Override
	protected DataSet toDataSet(DataSetInfo info) {
		return new LfsDataSet(this, info);
	}

	public final class LfsDataSet extends AbstractDataSet<LfsAvroDataSetServer> {
		public LfsDataSet(LfsAvroDataSetServer server, DataSetInfo info) {
			super(server, info);
		}
		
		public File getFile() {
			return new File(m_root, m_info.getId().substring(1));
		}

		@Override
		public RecordStream read() {
			return MultiFileAvroReader.scan(m_root).read();
		}

		@Override
		public long write(RecordStream stream) {
			File file = getFile();
			File partFile = new File(file, UUID.randomUUID().toString() + ".avro");
			return new AvroFileRecordWriter(partFile).write(stream);
		}
	
		@Override
		public long getLength() {
			return getFile().length();
		}
	}
}
