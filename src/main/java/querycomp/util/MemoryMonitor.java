package querycomp.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;

public class MemoryMonitor {
    private static final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    private static BufferedWriter logWriter;
    private static long startTime; // 记录程序启动时的时间

    // 初始化
    static {
        startTime = System.currentTimeMillis(); // 获取程序启动时的时间戳
    }

    // 设置日志输出文件
    public static void setLogFile(File file) throws IOException {
        if (!file.exists()) {
            file.createNewFile();  // 文件不存在时创建文件
        }
        logWriter = new BufferedWriter(new FileWriter(file));  // 追加模式写入文件
    }

    // 打印整个程序（JVM）的内存使用情况
    public static long printMemoryUsage(String stage) {
        if (logWriter == null) {
            System.err.println("Log file is not set. Please call setLogFile() first.");
            return 0;
        }
        MemoryUsage heapUsage = memoryMXBean.getHeapMemoryUsage();
        long usedMemory = heapUsage.getUsed();
        long maxMemory = heapUsage.getMax();
        long elapsedTime = System.currentTimeMillis() - startTime; // 计算距离程序启动的时间

        String logMessage = String.format("[%s] 当前堆内存使用: %d MB / %d MB, 距离启动时间: %d ms%n",
                stage, usedMemory / (1024 * 1024), maxMemory / (1024 * 1024), elapsedTime);
        
        try {
            logWriter.write(logMessage);
            logWriter.flush();  // 立即将内容写入文件
        } catch (IOException e) {
            System.err.println("Error writing to log file: " + e.getMessage());
        }
        return usedMemory;
    }

    // 关闭日志文件
    public static void closeLogFile() {
        if (logWriter != null) {
            try {
                logWriter.close();
            } catch (IOException e) {
                System.err.println("Error closing log file: " + e.getMessage());
            }
        }
    }
}

