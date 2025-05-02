package querycomp.db;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;

public interface NativeLibrary extends Library {
    NativeLibrary INSTANCE = Native.load(
        Platform.isWindows() ? "kernel32" : "c",
        NativeLibrary.class
    );

    // Linux: pthread_t pthread_self(void);
    long pthread_self();

    // Windows: HANDLE GetCurrentThread(void);
    Pointer GetCurrentThread();
}



