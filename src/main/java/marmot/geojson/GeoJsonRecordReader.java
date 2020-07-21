package marmot.geojson;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.List;

import org.geotools.feature.FeatureIterator;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.geojson.geom.GeometryJSON;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.Record;
import marmot.RecordReader;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.RecordStreamException;
import marmot.shp.ShapefileDataSets;
import marmot.stream.AbstractRecordStream;
import marmot.stream.MultiSourcesRecordStream;
import marmot.type.GeometryDataType;
import utils.func.Try;
import utils.geo.util.CRSUtils;
import utils.io.FileUtils;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GeoJsonRecordReader implements RecordReader {
	private static final Logger s_logger = LoggerFactory.getLogger(GeoJsonRecordReader.class);
	
	private final File m_start;
	private final Charset m_charset;
	
	private final List<File> m_files;
	private final RecordSchema m_schema;
	
	public static RecordReader from(File start, Charset charset) {
		return new GeoJsonRecordReader(start, charset);
	}
	
	private GeoJsonRecordReader(File start, Charset charset) {
		m_start = start;
		m_charset = charset;
		
		try {
			m_files = FileUtils.walk(start, "**/*.geojson").toList();
			if ( m_files.isEmpty() ) {
				throw new IllegalArgumentException("no GeoJson files to read: path=" + start);
			}

			m_schema = parseGeoJson(m_files.get(0), m_charset).getRecordSchema();
			s_logger.info("loading GeoJsonFile: from={}, nfiles={}", start, m_files.size());
		}
		catch ( Exception e ) {
			throw new RecordStreamException("fails to load GeoJSON: start=" + start,  e);
		}
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}

	@Override
	public RecordStream read() {
		return new StreamImpl(m_schema, m_files, m_charset);
	}
	
	@Override
	public String toString() {
		return String.format("%s: file=%s", getClass().getSimpleName(), m_start);
	}
	
	private static RecordStream parseGeoJson(File file, Charset charset) throws IOException, FactoryException {
        FeatureJSON fjson = new FeatureJSON(new GeometryJSON());
        
        String srid = null;
        try ( BufferedReader reader = Files.newBufferedReader(file.toPath(), charset) ) {
            CoordinateReferenceSystem crs = fjson.readCRS(reader);
            if ( crs != null ) {
            	srid = CRSUtils.toEPSG(crs);
            }
        }
        
        BufferedReader reader = Files.newBufferedReader(file.toPath(), charset);
		FeatureIterator<SimpleFeature> iter = fjson.streamFeatureCollection(reader);
		RecordStream strm = ShapefileDataSets.toRecordStream(iter)
											.onClose(() -> Try.run(reader::close))
											.project("geometry as the_geom, *-{geometry}");
		if ( srid != null ) {
			strm = new GeoJSoNStream(strm, srid);
		}
		
		return strm;
	}
	
	static class GeoJSoNStream extends AbstractRecordStream {
		private final RecordStream m_source;
		private final RecordSchema m_schema;
		
		GeoJSoNStream(RecordStream source, String srid) {
			m_source = source;
			
			GeometryDataType gType = (GeometryDataType)m_source.getRecordSchema()
																.getColumn("the_geom").type();
			m_schema = m_source.getRecordSchema().toBuilder()
								.addOrReplaceColumn("the_geom", gType.duplicate(srid))
								.build();
		}

		@Override
		protected void closeInGuard() throws Exception {
			m_source.close();
		}

		@Override
		public RecordSchema getRecordSchema() {
			return m_schema;
		}

		@Override
		public Record next() {
			return m_source.next();
		}

		@Override
		public Record nextCopy() {
			return m_source.nextCopy();
		}
	}

	static class StreamImpl extends MultiSourcesRecordStream<File> {
		private static final Logger s_logger = LoggerFactory.getLogger(StreamImpl.class);

		private final Charset m_charset;

		public StreamImpl(RecordSchema schema, List<File> files, Charset charset) {
			super(FStream.from(files), schema);
			
			m_charset = charset;
			setLogger(s_logger);
		}

		@Override
		protected RecordStream read(File geojsonFile) throws RecordStreamException {
			getLogger().debug("loading GeoJSoN: file=" + geojsonFile);
			
			try {
				return parseGeoJson(geojsonFile, m_charset);
			}
			catch ( Exception e ) {
				throw new RecordStreamException("fails to read a shapefile: file=" + geojsonFile, e);
			}
		}
	}
}
