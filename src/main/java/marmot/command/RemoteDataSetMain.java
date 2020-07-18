package marmot.command;

import marmot.remote.client.GrpcMarmotRuntimeProxy;
import picocli.CommandLine.Command;


/**
 * </ol>
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="mr_dataset",
		parameterListHeading = "Parameters:%n",
		optionListHeading = "Options:%n",
		description="dataset-related commands",
		subcommands = {
			DatasetCommands.ListDataSet.class,
			DatasetCommands.Show.class,
			DatasetCommands.Schema.class,
//			DatasetCommands.Move.class,
//			DatasetCommands.SetGcInfo.class,
//			DatasetCommands.AttachGeometry.class,
//			DatasetCommands.Count.class,
//			DatasetCommands.Bind.class,
			DatasetCommands.Delete.class,
			DatasetCommands.Import.class,
//			DatasetCommands.Export.class,
			UploadFilesCommand.class,
//			DatasetCommands.Thumbnail.class,
		})
public class RemoteDataSetMain extends RemoteMarmotCommand {
	public static final void main(String... args) throws Exception {
		run(new RemoteDataSetMain(), args);
	}

	@Override
	protected void run(GrpcMarmotRuntimeProxy marmot) throws Exception { }
}
