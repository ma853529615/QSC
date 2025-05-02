package querycomp.db;

import java.util.Vector;

public class BlockMeta {
    int size;
    int relationId;
    int blockId;
    Vector<Integer[]> block;
    public BlockMeta(int size, int relationId, int blockId, Vector<Integer[]> block) {
        this.size = size;
        this.relationId = relationId;
        this.blockId = blockId;
        this.block = block;
    }
    public String toString() {
        String block_str = "[";
        for(int i=0;i<block.size();i++){
            block_str += "[";
            for(int j=0;j<block.get(i).length;j++){
                block_str += block.get(i)[j];
                if(j<block.get(i).length-1){
                    block_str += ",";
                }
            }
            block_str += "]";
            if(i<block.size()-1){
                block_str += ",";
            }
        }
        block_str += "]";
        return block_str;
    }
}
