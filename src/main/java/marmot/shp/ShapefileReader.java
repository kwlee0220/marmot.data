package marmot.shp;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import marmot.RecordReader;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.RecordStreamException;
import utils.geo.Shapefile;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ShapefileReader implements RecordReader {
	private static final Logger s_logger = LoggerFactory.getLogger(ShapefileReader.class);
	
	private final File m_start;
	private final Charset m_charset;

	private SimpleFeatureType m_sfType;
	private final RecordSchema m_schema;
	private final List<File> m_shpFiles;
	
	public static ShapefileReader from(File start, Charset charset) {
		return new ShapefileReader(start, charset);
	}
	
	private ShapefileReader(File start, Charset charset) {
		m_start = start;
		m_charset = charset;
		
		try {
			m_shpFiles = Shapefile.traverseShpFiles(start).toList();
			if ( m_shpFiles.isEmpty() ) {
				throw new IllegalArgumentException("no Shapefiles to read: path=" + start);
			}
			
			m_schema = loadRecordSchema(m_shpFiles.get(0));
			s_logger.info("loading {}: nfiles={}", this, m_shpFiles.size());
		}
		catch ( IOException e ) {
			throw new RecordStreamException("fails to load ShapefileRecordStream: start=" + start,  e);
		}
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	public SimpleFeatureType getSimpleFeatureType() {
		return m_sfType;
	}

	@Override
	public RecordStream read() {
		return new ShapefileRecordStream(m_schema, Lists.newArrayList(m_shpFiles), m_charset);
	}
	
	@Override
	public String toString() {
		return String.format("%s: path=%s, charset=%s", getClass().getSimpleName(), m_start, m_charset);
	}
	
	private RecordSchema loadRecordSchema(File shpFile) throws IOException {
		s_logger.debug("loading Shapefile: {}", shpFile);
		SimpleFeatureType sfType = Shapefile.of(shpFile, m_charset).getSimpleFeatureType();
		return ShapefileDataSets.toRecordSchema(sfType);
	}
}
