package querycomp;

import org.apache.commons.cli.*;
import querycomp.QueryComp;

public class Main {

    public static final String DEFAULT_PATH = ".";
    public static final int DEFAULT_THREADS = 1;
    public static final int HELP_WIDTH = 125;
    public static final int DEFAULT_FREQUENCY = 500;
    public static final int DEFAULT_RULE_LEN = 3;
    public static final int DEFAULT_MINE_TIME = 30;
    public static final String DEFAULT_DB_TYPE = "duckdb";
    public static final String DEFAULT_DB_INFO = "./dbfile";
    public static final String DEFAULT_COMPRESS_METHOD = "method";
    public static final String DEFAULT_STATS_FILENAME = "stats.csv";
    public static final String DEFAULT_ACQUISITION_FUNCTION = "UCB";
    public static final int DEFAULT_ALPHA = 20;


    private static final String SHORT_OPT_HELP = "h";
    private static final String SHORT_OPT_INPUT = "I";
    private static final String SHORT_OPT_OUTPUT = "O";
    private static final String SHORT_OPT_FREQUENCY = "F";
    private static final String SHORT_OPT_RULE_LEN = "L";
    private static final String SHORT_OPT_CONSTANT = "c";
    private static final String SHORT_OPT_THREAD = "t";
    private static final String SHORT_OPT_VALIDATE = "v";
    private static final String SHORT_OPT_MINE_TIME = "m";
    private static final String SHORT_OPT_COMPMETHOD = "M";
    private static final String SHORT_OPT_ONLY_VALIDATION = "V";
    private static final String SHORT_OPT_BASE = "B";
    private static final String SHORT_OPT_STATS = "s";
    private static final String SHORT_OPT_ALPHA = "a";
    private static final String SHORT_OPT_BENCH = "b";
    private static final String SHORT_OPT_DB_TYPE = "d";
    private static final String SHORT_OPT_DB_INFO = "i";
    private static final String SHORT_OPT_ACQUISITION_FUNCTION = "A";
    private static final String LONG_OPT_HELP = "help";
    private static final String LONG_OPT_INPUT = "input";
    private static final String LONG_OPT_OUTPUT = "output";
    private static final String LONG_OPT_THREAD = "thread";
    private static final String LONG_OPT_VALIDATE = "validate";
    private static final String LONG_OPT_FREQUENCY = "frequency";
    private static final String LONG_OPT_RULE_LEN = "rulelen";
    private static final String LONG_OPT_CONSTANT = "constant";
    private static final String LONG_OPT_MINE_TIME = "minetime";
    private static final String LONG_OPT_COMPMETHOD = "method";
    private static final String LONG_OPT_STATS = "stats";
    private static final String LONG_OPT_ALPHA = "alpha";
    private static final String LONG_OPT_ONLY_VALIDATION = "onlyval";
    private static final String LONG_OPT_BASE = "base";
    private static final String LONG_OPT_BENCH = "bench";
    private static final String LONG_OPT_DB_TYPE = "db";
    private static final String LONG_OPT_DB_INFO = "dbinfo";
    private static final String LONG_OPT_ACQUISITION_FUNCTION = "acquisition_function";


    private static final Option OPTION_HELP = Option.builder(SHORT_OPT_HELP).longOpt(LONG_OPT_HELP)
            .desc("Display this help").build();
    private static final Option OPTION_INPUT_PATH = Option.builder(SHORT_OPT_INPUT).longOpt(LONG_OPT_INPUT)
            .numberOfArgs(2).argName("path> <name").type(String.class)
            .desc("The path to the input KB and the name of the KB").build();
    private static final Option OPTION_OUTPUT_PATH = Option.builder(SHORT_OPT_OUTPUT).longOpt(LONG_OPT_OUTPUT)
            .numberOfArgs(2).argName("path> <name").type(String.class)
            .desc("The path to where the output/compressed KB is stored and the name of the output KB").build();
    private static final Option OPTION_THREAD = Option.builder(SHORT_OPT_THREAD).longOpt(LONG_OPT_THREAD)
            .argName("#threads").hasArg().type(Integer.class).desc("The number of threads").build();
    private static final Option OPTION_VALIDATE = Option.builder(SHORT_OPT_VALIDATE).longOpt(LONG_OPT_VALIDATE)
            .desc("Validate result after compression").build();
    private static final Option OPTION_FREQUENCY = Option.builder(SHORT_OPT_FREQUENCY).longOpt(LONG_OPT_FREQUENCY)
            .argName("frequency").hasArg().type(Integer.class).desc("The frequency threshold").build();
    private static final Option OPTION_RULE_LEN = Option.builder(SHORT_OPT_RULE_LEN).longOpt(LONG_OPT_RULE_LEN)
            .argName("rulelen").hasArg().type(Integer.class).desc("The rule length threshold").build();
    private static final Option OPTION_CONSTANT = Option.builder(SHORT_OPT_CONSTANT).longOpt(LONG_OPT_CONSTANT)
            .desc("The constant threshold").build();
    private static final Option OPTION_MINE_TIME = Option.builder(SHORT_OPT_MINE_TIME).longOpt(LONG_OPT_MINE_TIME)
            .argName("minetime").hasArg().type(Integer.class).desc("The minimum time threshold").build();
    private static final Option OPTION_COMP_METHOD = Option.builder(SHORT_OPT_COMPMETHOD).longOpt(LONG_OPT_COMPMETHOD)
            .argName("method").hasArg().type(String.class).desc("The compression method").build();
    private static final Option OPTION_STATS = Option.builder(SHORT_OPT_STATS).longOpt(LONG_OPT_STATS)
            .argName("stats").hasArg().type(String.class).desc("The stats filename").build();
    private static final Option OPTION_ALPHA = Option.builder(SHORT_OPT_ALPHA).longOpt(LONG_OPT_ALPHA)
            .argName("alpha").hasArg().type(Integer.class).desc("The alpha value").build();
    private static final Option OPTION_ONLY_VALIDATION = Option.builder(SHORT_OPT_ONLY_VALIDATION).longOpt(LONG_OPT_ONLY_VALIDATION)
            .desc("Only validate the result").build();
    private static final Option OPTION_BASE = Option.builder(SHORT_OPT_BASE).longOpt(LONG_OPT_BASE)
            .argName("base").hasArg().type(String.class).desc("The base path").build();
    private static final Option OPTION_BENCH = Option.builder(SHORT_OPT_BENCH).longOpt(LONG_OPT_BENCH)
            .desc("Run the benchmark").build();
    private static final Option OPTION_DB_TYPE = Option.builder(SHORT_OPT_DB_TYPE).longOpt(LONG_OPT_DB_TYPE)
            .argName("db").hasArg().type(String.class).desc("The db type").build();
    private static final Option OPTION_DB_INFO = Option.builder(SHORT_OPT_DB_INFO).longOpt(LONG_OPT_DB_INFO)
            .argName("dbinfo").hasArg().type(String.class).desc("The db info").build();
    private static final Option OPTION_ACQUISITION_FUNCTION = Option.builder(SHORT_OPT_ACQUISITION_FUNCTION).longOpt(LONG_OPT_ACQUISITION_FUNCTION)
            .argName("acquisition_function").hasArg().type(String.class).desc("The acquisition function").build();
    public static void main(String[] args) throws Exception {
        Options options = buildOptions();
        QueryComp queryComp = parseArgs(options, args);
        if (null != queryComp) {
            queryComp.run();
        }
        System.exit(0);
    }

    protected static QueryComp parseArgs(Options options, String[] args) throws Exception {
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        /* Help */
        if (cmd.hasOption(OPTION_HELP)) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(HELP_WIDTH);
            formatter.printHelp("java -jar querycomp.jar", options, true);
            return null;
        }

        /* Input/Output */
        String input_path = DEFAULT_PATH;
        String output_path = DEFAULT_PATH;
        String input_kb_name = null;
        String output_kb_name = null;
        if (cmd.hasOption(OPTION_INPUT_PATH)) {
            String[] values = cmd.getOptionValues(OPTION_INPUT_PATH);
            input_path = values[0];
            input_kb_name = values[1];
            System.out.printf("Input path set to: %s/%s\n", input_path, input_kb_name);
        }
        if (cmd.hasOption(OPTION_OUTPUT_PATH)) {
            String[] values = cmd.getOptionValues(OPTION_OUTPUT_PATH);
            output_path = values[0];
            output_kb_name = values[1];
            System.out.printf("Output path set to: %s/%s\n", output_path, output_kb_name);
        }
        if (null == input_kb_name) {
            System.err.println("Missing input KB name");
            return null;
        }
        output_kb_name = (null == output_kb_name) ? input_kb_name + "_comp" : output_kb_name;


        /* Assign Run-time parameters */
        int threads = DEFAULT_THREADS;
        if (cmd.hasOption(OPTION_THREAD)) {
            String value = cmd.getOptionValue(OPTION_THREAD);
            if (null != value) {
                threads = Integer.parseInt(value);
                
            }
        }
        System.out.println("#Threads set to: " + threads);
        boolean validation = cmd.hasOption(SHORT_OPT_VALIDATE);
        
        int frequency = DEFAULT_FREQUENCY;
        if (cmd.hasOption(OPTION_FREQUENCY)) {
            String value = cmd.getOptionValue(OPTION_FREQUENCY);
            if (null != value) {
                frequency = Integer.parseInt(value);
                
            }
        }
        System.out.println("Frequency set to: " + frequency);
        int rule_len = DEFAULT_RULE_LEN;
        if (cmd.hasOption(OPTION_RULE_LEN)) {
            String value = cmd.getOptionValue(OPTION_RULE_LEN);
            if (null != value) {
                rule_len = Integer.parseInt(value);
                
            }
        }
        System.out.println("Rule length set to: " + rule_len);
        boolean constant = cmd.hasOption(OPTION_CONSTANT);
        if (constant) {
            System.out.println("Constant threshold set to: " + constant);
        }else{
            System.out.println("Constant threshold not set");
        }

        int minetime = DEFAULT_MINE_TIME;
        if (cmd.hasOption(OPTION_MINE_TIME)) {
            String value = cmd.getOptionValue(OPTION_MINE_TIME);
            if (null != value) {
                minetime = Integer.parseInt(value);
                
            }
        }
        System.out.println("Minimum time threshold set to: " + minetime);
        String compressMethod = DEFAULT_COMPRESS_METHOD;
        if (cmd.hasOption(OPTION_COMP_METHOD)) {
            String value = cmd.getOptionValue(OPTION_COMP_METHOD);
            if (null != value) {
                compressMethod = value;
                
            }
        }
        System.out.println("Compression method set to: " + compressMethod);
        String statsFilename = DEFAULT_STATS_FILENAME;
        if (cmd.hasOption(OPTION_STATS)) {
            String value = cmd.getOptionValue(OPTION_STATS);
            if (null != value) {
                statsFilename = value;
                
            }
        }
        System.out.println("Stats filename set to: " + statsFilename);
        int alpha = DEFAULT_ALPHA;
        if(cmd.hasOption(OPTION_ALPHA)){
            String value = cmd.getOptionValue(OPTION_ALPHA);
            if (null != value) {
                alpha = Integer.parseInt(value);
                
            }
        }
        boolean only_val = cmd.hasOption(SHORT_OPT_ONLY_VALIDATION);
        if(only_val){
            System.out.println("Only validate the result");
        }{
            System.out.println("Validate the result after compression");
        }
        String base = DEFAULT_PATH;
        if(cmd.hasOption(SHORT_OPT_BASE)){
            base = cmd.getOptionValue(SHORT_OPT_BASE);
        }
        System.out.println("Base path set to: " + base);

        boolean bench = cmd.hasOption(SHORT_OPT_BENCH);
        if(bench){
            System.out.println("Run the benchmark");
        }
        String db_type = DEFAULT_DB_TYPE;
        if(cmd.hasOption(SHORT_OPT_DB_TYPE)){
            db_type = cmd.getOptionValue(SHORT_OPT_DB_TYPE);
        }
        System.out.println("DB type set to: " + db_type);
        String db_info = DEFAULT_DB_INFO;
        if(cmd.hasOption(SHORT_OPT_DB_INFO)){
            db_info = cmd.getOptionValue(SHORT_OPT_DB_INFO);
        }
        System.out.println("DB info set to: " + db_info);
        String acquisition_function = DEFAULT_ACQUISITION_FUNCTION;
        if(cmd.hasOption(SHORT_OPT_ACQUISITION_FUNCTION)){
            acquisition_function = cmd.getOptionValue(SHORT_OPT_ACQUISITION_FUNCTION);
        }
        System.out.println("Acquisition function set to: " + acquisition_function);
        QueryCompConfig config = new QueryCompConfig(
                input_path, input_kb_name, output_path, output_kb_name,
                threads, validation, frequency, rule_len, constant, minetime,
                compressMethod, statsFilename, alpha, only_val, base, bench, db_type, db_info,
                acquisition_function
        );
        return new QueryComp(config);
    }

    protected static Options buildOptions() {
        Options options = new Options();

        /* Help */
        options.addOption(OPTION_HELP);

        /* Input/output options */
        options.addOption(OPTION_INPUT_PATH);
        options.addOption(OPTION_OUTPUT_PATH);

        /* Run-time parameter options */
        options.addOption(OPTION_THREAD);
        options.addOption(OPTION_VALIDATE);
        options.addOption(OPTION_FREQUENCY);
        options.addOption(OPTION_RULE_LEN);

        options.addOption(OPTION_CONSTANT);
        options.addOption(OPTION_MINE_TIME);
        options.addOption(OPTION_COMP_METHOD);
        options.addOption(OPTION_STATS);
        options.addOption(OPTION_ALPHA);
        options.addOption(OPTION_ONLY_VALIDATION);
        options.addOption(OPTION_BASE);
        options.addOption(OPTION_BENCH);
        options.addOption(OPTION_DB_TYPE);
        options.addOption(OPTION_DB_INFO);
        options.addOption(OPTION_ACQUISITION_FUNCTION);

        return options;
    }
}
