package test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.sql.SQLOutput;

public class Jaa {

    public static void mains(String[] args) throws IOException {


        RandomAccessFile file = new RandomAccessFile("./doc/words.log","rw");
        FileChannel channel = file.getChannel();

        IntBuffer intBuffer = IntBuffer.allocate(10);
        ByteBuffer byteBuffer = ByteBuffer.allocate(48);

        int byteSizes = channel.read(byteBuffer);
        System.out.println("read" + byteSizes);
        byteBuffer.compact();
//
        while(byteBuffer.hasRemaining()){
            System.out.print((char)byteBuffer.get());
        }
//
//        System.out.println(byteBuffer.getChar());
    }

    public static void main(String[] args) {
        IntBuffer intBuffer = IntBuffer.allocate(10);

        LongBuffer longBuffer = LongBuffer.allocate(10);
        longBuffer.put(12L);
        longBuffer.put(13L);
        System.out.println(intBuffer.get());
    }
}
