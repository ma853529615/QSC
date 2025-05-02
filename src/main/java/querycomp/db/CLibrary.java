package querycomp.db;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;

public interface CLibrary extends Library {
    CLibrary INSTANCE = Native.load(
        Platform.isWindows() ? "kernel32" : "c",
        CLibrary.class
    );

    // Linux: int pthread_setaffinity_np(pthread_t thread, size_t cpusetsize, const cpu_set_t *cpuset);
    int pthread_setaffinity_np(long thread, long cpusetsize, Pointer cpuset);

    // Windows: DWORD_PTR SetThreadAffinityMask(HANDLE hThread, DWORD_PTR dwThreadAffinityMask);
    long SetThreadAffinityMask(Pointer hThread, long dwThreadAffinityMask);
}

