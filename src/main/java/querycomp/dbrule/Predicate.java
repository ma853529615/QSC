package querycomp.dbrule;

public class Predicate{
    public final String functor;
    public Argument[] args;
    public Integer duplicateID = 0;
    protected Predicate(String functor, Argument[] args_array){
        this.functor = functor;
        args= args_array;
    }
    public void setArg(int index, Argument arg){
        args[index] = arg;
    }
    public String[] getColumns(){
        String[] argNames = new String[args.length];
        for (int i = 0; i < args.length; i++){
            argNames[i] = functor+"_"+(i+1);
        }
        return argNames;
    }
    public int getArity(){
        return args.length;
    }
    public String fromCls(){
        if(duplicateID > 1){
            return ("\""+functor+"\"" + " AS " + bodySelCls());
        }else{
            return "\""+functor+"\"";
        }
    }
    public String getSQLName(){
        // consider duplicated predicates in a rule
        return fromCls();
    }
    public String bodySelCls(){
        if(duplicateID > 1){
            return "\""+(functor+ "_dup_" + duplicateID)+"\"";
        }else{
            return "\""+functor+"\"";
        }
    }
    public String getColumnName(int index){
        return bodySelCls()+"."+getSoleColumnName(index);
    } 
    public String getSoleColumnName(int index){
        // only column name
        return "\""+ functor + "_" + (index+1)+"\"";
    }
}