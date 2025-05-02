package querycomp.util;

import java.util.Comparator;
import java.util.TreeSet;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Iterator;
// public class ArrayTreeSet extends TreeSet<int[]> {
//     private static final Comparator<int[]> arrayComparator = (a, b) -> {
//         int len = Math.min(a.length, b.length);
//         for (int i = 0; i < len; i++) {
//             if (a[i] != b[i]) {
//                 return Integer.compare(a[i], b[i]);
//             }
//         }
//         return Integer.compare(a.length, b.length);
//     };

//     public ArrayTreeSet() {
//         super(arrayComparator);
//     }
// }

public class ArrayTreeSet implements Iterable<int[]> {
    private final HashSet<ArrayWrapper> set;

    public ArrayTreeSet() {
        this.set = new HashSet<>();
    }
    public ArrayTreeSet copy() {
        ArrayTreeSet newSet = new ArrayTreeSet();
        for(ArrayWrapper arrayWrapper : set) {
            newSet.add(arrayWrapper.getArray());
        }
        return newSet;
    }

    // 添加数组
    public boolean add(int[] array) {
        return set.add(new ArrayWrapper(array));
    }

    // 删除数组
    public boolean remove(int[] array) {
        return set.remove(new ArrayWrapper(array));
    }

    // 判断是否包含数组
    public boolean contains(int[] array) {
        return set.contains(new ArrayWrapper(array));
    }
    public boolean isEmpty(){
        return set.isEmpty();
    }
    // 添加多个数组
    public boolean addAll(ArrayTreeSet otherSet) {
        boolean changed = false;
        for (ArrayWrapper arrayWrapper : otherSet.set) {
            if (set.add(arrayWrapper)) {
                changed = true;
            }
        }
        return changed;
    }

    // 删除多个数组
    public boolean removeAll(ArrayTreeSet otherSet) {
        boolean changed = false;
        for (ArrayWrapper arrayWrapper : otherSet.set) {
            if (set.remove(arrayWrapper)) {
                changed = true;
            }
        }
        return changed;
    }
    public boolean containsAny(ArrayTreeSet otherSet) {
        for (ArrayWrapper arrayWrapper : otherSet.set) {
            if (set.contains(arrayWrapper)) {
                return true;
            }
        }
        return false;
    }
    // 判断是否包含多个数组
    public boolean containsAll(ArrayTreeSet otherSet) {
        for (ArrayWrapper arrayWrapper : otherSet.set) {
            if (!set.contains(arrayWrapper)) {
                return false;
            }
        }
        return true;
    }

    // 克隆集合
    @Override
    public ArrayTreeSet clone() {
        ArrayTreeSet clonedSet = new ArrayTreeSet();
        clonedSet.set.addAll(this.set);  // 复制现有的所有元素
        return clonedSet;
    }

    // 获取集合的大小
    public int size() {
        return set.size();
    }

    // 支持 for-each 迭代
    @Override
    public Iterator<int[]> iterator() {
        return new Iterator<int[]>() {
            private final Iterator<ArrayWrapper> it = set.iterator();

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
    public boolean intersect(ArrayTreeSet otherSet) {
        boolean changed = false;
        HashSet<ArrayWrapper> newSet = new HashSet<>();
        for (ArrayWrapper arrayWrapper : otherSet.set) {
            if (set.contains(arrayWrapper)) {
                newSet.add(arrayWrapper);
            }
        }
        if (newSet.size() != set.size()) {
            changed = true;
            set.clear();
            set.addAll(newSet);
        }
        return changed;
    }
    public void clear() {
        set.clear();
    }
}


