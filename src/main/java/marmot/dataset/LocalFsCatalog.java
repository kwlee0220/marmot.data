package marmot.dataset;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import utils.jdbc.JdbcProcessor;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LocalFsCatalog extends Catalog {
	private final File m_fsRoot;
	
	public LocalFsCatalog(File fsRoot, JdbcProcessor jdbc) {
		super(jdbc);
		
		m_fsRoot = fsRoot;
	}
	
	@Override
	public String toFilePath(String id) {
		id = Catalogs.normalize(id);
		return new File(m_fsRoot, id).toString();
	}
	
	public static LocalFsCatalog createCatalog(File fsRoot, JdbcProcessor jdbc) throws IOException {
		createCatalog(jdbc);
		
		FileUtils.deleteDirectory(fsRoot);
		FileUtils.forceMkdir(fsRoot);
		
		return new LocalFsCatalog(fsRoot, jdbc);
	}
	
	public static void dropCatalog(JdbcProcessor jdbc) {
		Catalog.dropCatalog(jdbc);;
	}
}
