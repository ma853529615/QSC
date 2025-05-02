package querycomp.db;

public class QueryInfo {
    public int headFucID;
    public int queryID=0;
    public int headArity;
    public int bodySize;
    public int[] bodyFucIDs;
    public int[] bodyArities;
    public String query;
    public String rule_query;
    public int id=-1;
    public boolean activate = true;
    public QueryInfo() {
    }
    public QueryInfo(int headFucID, int headArity, int bodySize, int[] bodyFucIDs, int[] bodyArities) {
        this.headFucID = headFucID;
        this.headArity = headArity;
        this.bodySize = bodySize;
        this.bodyFucIDs = bodyFucIDs;
        this.bodyArities = bodyArities;
    }
}
