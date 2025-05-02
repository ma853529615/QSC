package querycomp;

public class QueryCompConfig {
    /* I/O configurations */
    /** The path to the directory where the kb is located */
    public final String basePath;
    /** The name of the KB */
    public final String kbName;
    /** The path where the compressed KB should be stored */
    public final String dumpPath;
    /** The name of the dumped KB */
    public final String dumpName;
    /* Runtime Config */
    public String db_type;
    public String db_info;
    
    public int threads;
    /** Whether the compressed KB is recovered to check the correctness */
    public boolean validation;

    public int frequency;
    public int ruleLen;
    public boolean constant;
    public int mineTime;
    public String compressMethod;
    public String statsFilename;
    public int alpha;
    public boolean only_val;
    public String base;
    public boolean bench;
    public String acquisition_function;
    public QueryCompConfig(
            String basePath, String kbName, String dumpPath, String dumpName, int threads, boolean validation,
             int frequency, int ruleLen, boolean constant, int mineTime, String compressMethod, String statsFilename,
             int alpha, boolean only_val, String base, boolean bench, String db_type, String db_info, String acquisition_function
    ) {
        this.basePath = basePath;
        this.kbName = kbName;
        this.dumpPath = dumpPath;
        this.dumpName = dumpName;
        this.threads = Math.max(1, threads);
        this.validation = validation;
        this.frequency = frequency;
        this.ruleLen = ruleLen;
        this.constant = constant;
        this.mineTime = mineTime;
        this.compressMethod = compressMethod;
        this.statsFilename = statsFilename;
        this.alpha = alpha;
        this.only_val = only_val;
        this.base = base;
        this.bench = bench;
        this.db_type = db_type;
        this.db_info = db_info;
        this.acquisition_function = acquisition_function;
    }
}
