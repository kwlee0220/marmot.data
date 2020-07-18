package marmot.shp;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import org.geotools.data.shapefile.ShapefileDataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.RecordStreamException;
import marmot.stream.ChainedRecordStream;
import utils.geo.Shapefile;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ShapefileRecordStream extends ChainedRecordStream {
	private static final Logger s_logger = LoggerFactory.getLogger(ShapefileRecordStream.class);

	private final RecordSchema m_schema;
	private final Charset m_charset;
	private final List<File> m_shpFiles;
	private File m_currentFile;
	
	public ShapefileRecordStream(RecordSchema schema, List<File> shpFiles, Charset charset) {
		m_schema = schema;
		m_shpFiles = shpFiles;
		m_charset = charset;
		setLogger(s_logger);
	}
	
	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	public Charset getCharset() {
		return m_charset;
	}
	
	@Override
	public String toString() {
		return String.format("Shapefiles[current=%s]", m_currentFile);
	}

	@Override
	protected RecordStream getNextRecordStream() {
		if ( m_shpFiles.isEmpty() ) {
			return null;
		}

		m_currentFile = m_shpFiles.remove(0);
		try {
			return readShpFile(m_currentFile);
		}
		catch ( Exception e ) {
			throw new RecordStreamException("fails to read Shapefile: path=" + m_currentFile, e);
		}
	}
	
	private RecordStream readShpFile(File shpFile) throws IOException {
		getLogger().info("loading shapefile: " + shpFile);
		ShapefileDataStore shpStore = Shapefile.of(shpFile, m_charset).getDataStore();
		
		return ShapefileDataSets.toRecordStream(shpStore.getFeatureSource())
								.onClose(shpStore::dispose);
	}
}
