package querycomp.util;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

public class ConcurrentArrayTreeSet extends ArrayTreeSet {
    private final ConcurrentHashMap<ArrayWrapper, Boolean> concurrentMap;

    public ConcurrentArrayTreeSet() {
        // 使用 ConcurrentHashMap 确保线程安全并支持并发写入
        this.concurrentMap = new ConcurrentHashMap<>();
    }

    @Override
    public boolean add(int[] array) {
        // 通过 ConcurrentHashMap 的 putIfAbsent 方法实现线程安全的并发写入
        return concurrentMap.putIfAbsent(new ArrayWrapper(array), Boolean.TRUE) == null;
    }

    @Override
    public Iterator<int[]> iterator() {
        return new Iterator<int[]>() {
            private final Iterator<ArrayWrapper> it = concurrentMap.keySet().iterator();

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public int[] next() {
                return it.next().getArray();
            }
        };
    }

    // 如果需要清除集合，也可以提供一个清除方法
    @Override
    public void clear() {
        concurrentMap.clear();
    }
}
