package ggcd.tp1;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

public final class App {
    private static final String PROGRAM_NAME = "tp1";

    @Parameter(
            names = {"-h", "--help"},
            help = true,
            description = "Displays help information")
    private boolean help = false;

    private App() {}

    public static void main(final String[] args) {
        new App().start(args);
    }

    public void start(final String[] args) {
        String command = this.parseArguments(args);

        try {
            switch (command) {
                case "to-parquet":
                    ToParquet.of().run();
                    break;
                case "movie-count":
                    MoviesByYear.of().run();
                    break;
                case "most-voted":
                    MostVotedByYear.of().run();
                    break;
                case "top10-ranking":
                    Top10RatingByYear.of().run();
                    break;

                case "from-parquet":
                    AvroParquetToJSON.of().run();

                case "movie-recomendation":
                    MovieRecomendation.of().run();

                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String parseArguments(final String[] args) {
        JCommander commands = new JCommander(this);
        commands.setProgramName(PROGRAM_NAME);
        commands.addCommand("to-parquet", ToParquet.of());
        commands.addCommand("movie-count", MoviesByYear.of());
        commands.addCommand("most-voted", MostVotedByYear.of());
        commands.addCommand("top10-ranking", Top10RatingByYear.of());

        commands.addCommand("from-parquet", AvroParquetToJSON.of());

        commands.addCommand("movie-recomendation", MovieRecomendation.of());

        try {
            commands.parse(args);

            if (this.help) {
                commands.usage();
                System.exit(0);
            }

            return commands.getParsedCommand();
        } catch (ParameterException exception) {
            System.err.println(exception.getMessage());
            commands.usage();
            System.exit(1);
        }

        return null;
    }
}
