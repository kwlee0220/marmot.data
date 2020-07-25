package marmot.command;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import marmot.MarmotLfsServer;
import marmot.remote.server.GrpcDataSetServiceServant;
import marmot.remote.server.GrpcFileServiceServant;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import utils.NetUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="marmot_grpc_lfs_dataset_server",
			parameterListHeading = "Parameters:%n",
			optionListHeading = "Options:%n",
			description="create a remote HDFS DataSetServer (using gRPC)")
public class GrpcLfsMarmotServerMain extends MarmotLocalFsCommand {
	private final static Logger s_logger = LoggerFactory.getLogger(GrpcLfsMarmotServerMain.class);
	
	private static final int DEFAULT_MARMOT_PORT = 15685;
	
	@Option(names={"-port"}, paramLabel="number", required=false,
			description={"marmot DataSetServer port number"})
	private int m_port = -1;
	
	public static final void main(String... args) throws Exception {
		run(new GrpcLfsMarmotServerMain(), args);
	}
	
	@Override
	protected void run(MarmotLfsServer marmot) throws Exception {
		int port = m_port;
		if ( port < 0 ) {
			String portStr = System.getenv("MARMOT_GRPC_PORT");
			port = (portStr != null) ? Integer.parseInt(portStr) : DEFAULT_MARMOT_PORT;
		}
		
		GrpcDataSetServiceServant servant = new GrpcDataSetServiceServant(marmot.getDataSetServer());
		GrpcFileServiceServant fileServant = new GrpcFileServiceServant(marmot.getFileServer());

		Server server = NettyServerBuilder.forPort(port)
											.addService(servant)
											.addService(fileServant)
											.build();
		server.start();

		if ( m_verbose ) {
			String host = NetUtils.getLocalHostAddress();
			System.out.printf("started: GrpcHdfsMarmotServer[host=%s, port=%d]%n", host, port);
		}
		server.awaitTermination();
	}
}
