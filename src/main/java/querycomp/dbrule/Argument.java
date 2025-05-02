package querycomp.dbrule;

import java.util.Arrays;
import java.util.Objects;
import java.util.Vector;

public class Argument{
    public static Vector<String> VAR_ARRAY = new Vector<>();
    public static final Vector<String> DEFAULT_VAR_ARRAY = new Vector<>(Arrays.asList("X", "Y", "Z", "A", "B", "C", "D", "E", "F", "G", "H"));  
    public boolean isConstant;
    protected final int predicate_id;
    protected final int position;
    public String name;
    public Argument(String name, boolean isConstant, int predicate_id, int position){
        this.name = name;
        this.isConstant = isConstant;
        if(!isConstant){
            VAR_ARRAY.add(name);
        }
        this.predicate_id = predicate_id;
        this.position = position;
    }
    public Argument(String name, int predicate_id, int position){
        this.name = name;
        this.isConstant = !checkVariable(name);
        if(!isConstant){
            VAR_ARRAY.add(name);
        }
        this.predicate_id = predicate_id;
        this.position = position;
    }
    public Argument(int name, int predicate_id, int position){
        this.name = name+"";
        this.isConstant = true;
        this.predicate_id = predicate_id;
        this.position = position;
    }
    static boolean checkVariable(String name){
        // if name start with digit, it is constant
        return !Character.isDigit(name.charAt(0));
    }
    // protected boolean equals(Argument another){
    //     return this.name == another.name && this.isConstant == another.isConstant;
    // }
    @Override
    public int hashCode() {
        // return Objects.hash(name, isConstant, predict_id);
        return Objects.hash(name);
    }

    // override equals method
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Argument argument = (Argument) obj;
        return name.equals(argument.name);
    }
    public int toInt(){
        if(isConstant){
            return Integer.parseInt(name);
        }else{
            return -VAR_ARRAY.indexOf(name);
        }
    }
    public String getName(){
        // return "\""+name+"\"";
        return name;
    }
}