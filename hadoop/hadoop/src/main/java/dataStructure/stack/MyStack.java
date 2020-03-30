package dataStructure.stack;

import java.util.List;

/**
 * 自定义栈
 * @author dingchuangshi
 */
public class MyStack<E> {

    private transient Object[] elementData;

    private int count;

    private int maxCount;

    public MyStack(int capacity){
        maxCount = capacity;
        elementData = new Object[capacity];
    }

    public E pop(){
        count -- ;
        E obj;
        if(count < 0){
            throw new NullPointerException();
        }
        obj =  (E) elementData[count ];
        elementData[count] = null;
        return obj;
    }

    public E pre(){
        return (E) elementData[count - 1];
    }

    public void push(E e){
        count ++;
        if(count >= maxCount){
            throw new ArrayIndexOutOfBoundsException("not to add,because capacity not enough");
        }
        elementData[count - 1] = e;
    }

    public int size(){
        return count;
    }
}
