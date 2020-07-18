package marmot.command;

import marmot.MarmotLfsServer;
import marmot.command.LfsDataSetMain.Format;
import marmot.dataset.LfsAvroDataSetServer;
import picocli.CommandLine.Command;
import utils.PicocliSubCommand;


/**
 * </ol>
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="ml_dataset",
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
			Format.class,
		})
public class LfsDataSetMain extends MarmotLocalFsCommand {
	public static final void main(String... args) throws Exception {
		run(new LfsDataSetMain(), args);
	}

	@Override
	protected void run(MarmotLfsServer marmot) throws Exception { }

	@Command(name="format", description="format the dataset store")
	public static class Format extends PicocliSubCommand<MarmotLfsServer> {
		@Override
		public void run(MarmotLfsServer marmot) throws Exception {
			LfsAvroDataSetServer server = marmot.getDataSetServer();
			LfsAvroDataSetServer.format(server.getJdbcProcessor(), server.getRoot());
		}
	}
}
