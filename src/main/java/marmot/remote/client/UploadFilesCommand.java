package marmot.remote.client;

import java.io.File;

import utils.UsageHelp;
import utils.func.CheckedConsumer;
import utils.func.FOption;

import marmot.file.FileServer;
import marmot.file.UploadFiles;

import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class UploadFilesCommand implements CheckedConsumer<FileServer> {
	@Parameters(paramLabel="src-path", index="0", arity="1..1",
				description="path to the source local file (or directory)")
	private String m_srcPath;
	
	@Parameters(paramLabel="dest-path", index="1", arity="1..1",
				description="HDFS path to the destination directory")
	private String m_destPath;
	
	@Option(names= {"-g", "-glob"}, paramLabel="glob", description="path matcher")
	private String m_glob = null;
	
	@Mixin private UsageHelp m_help;
	
	@Override
	public void accept(FileServer server) throws Exception {
		UploadFiles upload = new UploadFiles(server, new File(m_srcPath), m_destPath);
		FOption.run(m_glob, () -> upload.glob(m_glob));
		upload.run();
	}
}
