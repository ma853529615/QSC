package querycomp;

import java.io.PrintWriter;


public class PerformanceMonitor {
    /** 1ms = 1000000ns */
    public static final int NANO_PER_MILL = 1000000;

    /* Time Monitors */
    public long kbLoadTime = 0;
    public long hypothesisMiningTime = 0;
    public long dependencyAnalysisTime = 0;
    public long dumpTime = 0;
    public long validationTime = 0;
    public long neo4jTime = 0;
    public long totalTime = 0;

    /* Basic Rule Mining Time Statistics (measured in nanoseconds) */
    public long fingerprintCreationTime = 0;
    public long pruningTime = 0;
    public long evalTime = 0;
    public long kbUpdateTime = 0;

    /* Mining Statics Monitors */
    public int kbFunctors = 0;
    public int kbConstants = 0;
    public int kbSize = 0;
    public int hypothesisRuleNumber = 0;
    public int hypothesisSize = 0;
    public int necessaryFacts = 0;
    public int counterexamples = 0;
    public int supplementaryConstants = 0;
    public int sccNumber = 0;
    public int sccVertices = 0;
    public int fvsVertices = 0;
    /** This member keeps track of the number of evaluated SQL queries */
    public int evaluatedSqls = 0;


    public void show(PrintWriter writer) {
        writer.println("\n### Monitored Performance Info ###\n");
        writer.println("--- Time Cost ---");
        writer.printf(
                "(ms) %10s %10s %10s %10s %10s %10s %10s\n",
                "Load", "Hypo", "Dep", "Dump", "Validate", "Neo4j", "Total"
        );
        writer.printf(
                "     %10d %10d %10d %10d %10d %10d %10d\n\n",
                kbLoadTime, hypothesisMiningTime, dependencyAnalysisTime, dumpTime, validationTime, neo4jTime, totalTime
        );

        writer.println("--- Basic Rule Mining Cost ---");
        writer.printf(
                "(ms) %10s %10s %10s %10s %10s\n",
                "Fp", "Prune", "Eval", "KB Upd", "Total"
        );
        writer.printf(
                "     %10d %10d %10d %10d %10d\n\n",
                fingerprintCreationTime / NANO_PER_MILL, pruningTime / NANO_PER_MILL, evalTime / NANO_PER_MILL, kbUpdateTime / NANO_PER_MILL,
                (fingerprintCreationTime + pruningTime + evalTime + kbUpdateTime) / NANO_PER_MILL
        );

        writer.println("--- Statistics ---");
        writer.printf(
                "# %10s %10s %10s %10s %10s %10s %10s %10s %10s %10s %10s %10s %10s %10s\n",
                "|P|", "|Σ|", "|B|", "|H|", "||H||", "|N|", "|A|", "|ΔΣ|", "#SCC", "|SCC|", "|FVS|", "Comp(%)", "#SQL", "#SQL/|H|"
        );
        writer.printf(
                "  %10d %10d %10d %10d %10d %10d %10d %10d %10d %10d %10d %10.2f %10d %10.2f\n\n",
                kbFunctors,
                kbConstants,
                kbSize,
                hypothesisRuleNumber,
                hypothesisSize,
                necessaryFacts,
                counterexamples,
                supplementaryConstants,
                sccNumber,
                sccVertices,
                fvsVertices,
                (necessaryFacts + counterexamples + hypothesisSize) * 100.0 / kbSize,
                evaluatedSqls,
                evaluatedSqls * 1.0 / hypothesisRuleNumber
        );
    }
}
