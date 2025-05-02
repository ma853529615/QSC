package querycomp.dbrule;

import java.util.stream.Stream;

public class ProgressBar {
    public char incomplete = '░'; // U+2591 Unicode Character 表示还没有完成的部分
    public char complete = '█'; // U+2588 Unicode Character 表示已经完成的部分
    public int total = 100;
    public StringBuilder builder = new StringBuilder();
    public void ProgressBar(){
        builder = new StringBuilder();
        Stream.generate(() -> incomplete).limit(total).forEach(builder::append);
    }
    public void update(int i){
        builder.replace(i, i+1, String.valueOf(complete));
        String progressBar = "\r" + builder;
        String percent = " "+(i+1)+"%%";
        System.out.printf(progressBar+percent);
    }
}
