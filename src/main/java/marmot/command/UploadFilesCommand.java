package marmot.command;

import java.io.File;

import marmot.MarmotRuntime;
import marmot.file.FileServer;
import marmot.file.UploadFiles;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import utils.PicocliSubCommand;
import utils.StopWatch;
import utils.UnitUtils;
import utils.func.Funcs;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="upload", description="upload files into HDFS filesystem")
public class UploadFilesCommand extends PicocliSubCommand<MarmotRuntime> {
	@Parameters(paramLabel="src-path", index="0", arity="1..1",
				description="path to the source local file (or directory)")
	private String m_srcPath;
	
	@Parameters(paramLabel="dest-path", index="1", arity="1..1",
				description="HDFS path to the destination directory")
	private String m_destPath;
	
	@Option(names= {"-g", "--glob"}, paramLabel="glob", description="path matcher")
	private String m_glob = null;

	@Option(names={"-f"}, description="force to create a dataset")
	private boolean m_force = false;

	@Override
	public void run(MarmotRuntime marmot) throws Exception {
		StopWatch watch = StopWatch.start();

		FileServer server = marmot.getFileServer();
		UploadFiles upload = new UploadFiles(server, new File(m_srcPath), m_destPath);
		Funcs.when(m_glob != null, () -> upload.glob(m_glob));
		long nbytes = upload.run();
		
		long velo = Math.round(nbytes / watch.getElapsedInFloatingSeconds());
		System.out.printf("uploaded: src=%s tar=%s, size=%s elapsed=%s, velo=%s/s%n",
							m_srcPath, m_destPath, nbytes, watch.getElapsedMillisString(),
							UnitUtils.toByteSizeString(velo));
	}
}