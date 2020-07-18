package marmot.file;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.StopWatch;
import utils.UnitUtils;
import utils.Utilities;
import utils.io.FileUtils;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class UploadFiles {
	private static final Logger s_logger = LoggerFactory.getLogger(UploadFiles.class);
	private static final int BLOCK_SIZE = (int)UnitUtils.parseByteSize("256kb");
	
	private final FileServer m_server;
	private final File m_start;
	private PathMatcher m_pathMatcher;
	private final String m_dest;
	
	public UploadFiles(FileServer server, File start, String dest) {
		Utilities.checkNotNullArgument(server, "FileServer is null");
		Utilities.checkNotNullArgument(start, "source file(or directory)");
		Utilities.checkNotNullArgument(dest, "destination directory path");
		
		m_server = server;
		m_start = start;
		m_dest = dest;
	}
	
	public UploadFiles glob(String glob) {
		m_pathMatcher = (glob != null)
						? FileSystems.getDefault().getPathMatcher("glob:" + glob)
						: null;
		return this;
	}
	
	public long run() throws IOException {
		if ( m_start.isFile() ) {
			return uploadFile(m_start);
		}
		else {
			return uploadDir(m_start);
		}
	}
	
	private long uploadFile(File file) throws FileNotFoundException, IOException {
		if ( m_pathMatcher != null && !m_pathMatcher.matches(file.toPath()) ) {
			return 0;
		}
		
		StopWatch watch = StopWatch.start();
		try ( InputStream is = new BufferedInputStream(new FileInputStream(file), BLOCK_SIZE) ) {
			long nbytes = m_server.writeFile(m_dest, is);
			
			if ( s_logger.isInfoEnabled() ) {
				watch.stop();
				String velo = UnitUtils.toByteSizeString(Math.round(nbytes / watch.getElapsedInFloatingSeconds()));
				s_logger.info("uploaded: src={}, tar={}, nbytes={}, elapsed={}, velo={}/s",
						file, m_dest, UnitUtils.toByteSizeString(nbytes), watch.getElapsedSecondString(), velo);
			}
			return nbytes;
		}
	}
	
	private long uploadDir(File start) throws IOException {
		FStream<Path> pathes = FileUtils.walk(start.toPath()).drop(1)
										.filter(Files::isRegularFile)
										.sort();
		if ( m_pathMatcher != null ) {
			pathes = pathes.filter(m_pathMatcher::matches);
		}
		
		String prefix = start.toPath().toAbsolutePath().toString();
		int prefixLen = prefix.length();
		
		long total = 0;
		StopWatch totalWatch = StopWatch.start();
		for ( Path path: pathes ) {
			StopWatch watch = StopWatch.start();
			
			String suffix = path.toAbsolutePath().toString().substring(prefixLen);
			if ( suffix.charAt(0) == '/' ) {
				suffix = suffix.substring(1);
			}
			String destPath = m_dest + "/" + suffix;
			
			try ( InputStream is = new BufferedInputStream(Files.newInputStream(path), BLOCK_SIZE) ) {
				long nbytes = m_server.writeFile(destPath, is);
				total += nbytes;

				if ( s_logger.isInfoEnabled() ) {
					watch.stop();
					
					s_logger.info("uploaded: src={}, tar={}, nbytes={}({}), elapsed={}({}), velo={}/s ({}/s)",
							path, destPath,
							UnitUtils.toByteSizeString(nbytes), UnitUtils.toByteSizeString(total),
							watch.getElapsedSecondString(), totalWatch.getElapsedSecondString(),
							toVeloString(nbytes, watch), toVeloString(total, totalWatch));
				}
			}
		}
		
		return total;
	}
	
	private static String toVeloString(long nbytes, StopWatch watch) {
		long velo = Math.round(nbytes / watch.getElapsedInFloatingSeconds());
		return UnitUtils.toByteSizeString(velo);
	}
}
