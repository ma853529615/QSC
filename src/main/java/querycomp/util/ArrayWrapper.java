package querycomp.util;

import java.util.Arrays;

// 内部包装类，仅在 ArrayTreeSet 中使用
public class ArrayWrapper {
    private final int[] array;

    public ArrayWrapper(int[] array) {
        this.array = array;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(array);  // 基于数组内容计算 hashCode
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ArrayWrapper that = (ArrayWrapper) obj;
        return Arrays.equals(array, that.array);  // 基于数组内容判断相等性
    }

    public int[] getArray() {
        return array;
    }
}
