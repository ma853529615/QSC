package querycomp.dbrule;

public class DBTuple {
    private int[] data;
    private int relation = -1;
    public int arity;

    public DBTuple(int[] data){
        this.data = data;
        this.relation = data[data.length-1];
        this.arity = data.length-1;
    }
    public void setRelationID(int relation){
        this.relation = relation;
    }
    public int getRelationID(){
        return relation;
    }
    public int getdata(int index){
        assert index < arity: "Index out of bound";
        return data[index];
    }
    public int[] toArray(){
        return data;
    }

    public int hashCode(){
        int hash = 0;
        for(int i = 0; i < arity+1; i++){
            hash = hash * 31 + data[i];
        }
        return hash;
    }
    public boolean equals(Object obj){
        if(obj instanceof DBTuple){
            DBTuple t = (DBTuple)obj;
            if(t.arity != arity){
                return false;
            }
            // for(int i = 0; i < tuple.length; i++){
            //     if(t.tuple[i] != tuple[i]){
            //         return false;
            //     }
            // }
            // if(t.hashCode() == hashCode()){
            //     return true;
            // }
            for(int i = 0; i < arity+1; i++){
                if(t.data[i] != data[i]){
                    return false;
                }
            }
            return true;
        }
        return false;
    }
    public String toString(){
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < arity+1; i++){
            sb.append(data[i]);
            if(i < arity){
                sb.append(", ");
            }
        }
        return sb.toString();
    }
}
