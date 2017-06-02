package io.mycat.net2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * Created by zcg on 2017/5/31.
 */
public class DirectConDataBuffer implements ConDataBuffer {

    private SharedBufferPool sharedBufferPool;
    private ByteBuffer byteBuffer;
    private int readPos;
    private int writePos;

    public DirectConDataBuffer(SharedBufferPool sharedBufferPool) {
        this.sharedBufferPool = sharedBufferPool;
        this.byteBuffer = sharedBufferPool.allocate();
        this.readPos = 0;
        this.writePos = 0;
    }

    public DirectConDataBuffer() {
        this.byteBuffer = ByteBuffer.allocate(300);
        this.readPos = 0;
        this.writePos = 0;
    }

    @Override
    public int transferFrom(SocketChannel socketChanel) throws IOException {
        byteBuffer.clear();
        readPos = 0;
        int readNum = socketChanel.read(byteBuffer);
        if (readNum>0) {
            writePos = readPos + readNum;
        }
        return readNum;
    }

    @Override
    public void putBytes(ByteBuffer buf) throws IOException {
        byteBuffer.clear();
        readPos = 0;
        byteBuffer.put(buf);
        writePos = byteBuffer.position();
    }

    @Override
    public void putBytes(byte[] buf) throws IOException {
        byteBuffer.clear();
        readPos = 0;
        byteBuffer.put(buf);
        writePos = byteBuffer.position();
    }

    @Override
    public ByteBuffer beginWrite(int length) throws IOException {
        byteBuffer.clear();
        readPos = 0;
        writePos = 0;
        if (length <= byteBuffer.capacity()) {
            byteBuffer.limit(length);
        } else {
            byteBuffer.limit(byteBuffer.capacity());
        }
        return byteBuffer.slice();
    }

    @Override
    public void endWrite(ByteBuffer buffer) throws IOException {
        writePos = buffer.position();
    }

    @Override
    public byte getByte(int index) throws IOException {
        return byteBuffer.get(index);
    }

    @Override
    public ByteBuffer getBytes(int index, int length) throws IOException {
        int p = byteBuffer.position();
        int l = byteBuffer.limit();
        byteBuffer.position(index);
        byteBuffer.limit(index + length);
        ByteBuffer buffer = byteBuffer.slice();
        byteBuffer.limit(l);
        byteBuffer.position(p);
        return buffer;
    }

    @Override
    public int transferTo(SocketChannel socketChanel) throws IOException {
        byteBuffer.position(readPos);
        byteBuffer.limit(writePos);
        int writeNum = socketChanel.write(byteBuffer);
        if (writeNum>0) {
            readPos += writeNum;
        }
        return writeNum;
    }

    @Override
    public int writingPos() throws IOException {
        return writePos;
    }

    @Override
    public int readPos() {
        return readPos;
    }

    @Override
    public int totalSize() {
        return byteBuffer.capacity();
    }

    @Override
    public void setWritingPos(int writingPos) throws IOException {
        this.writePos = writingPos;
    }

    @Override
    public void setReadingPos(int readingPos) {
        this.readPos = readingPos;
    }

    @Override
    public boolean isFull() throws IOException {
        return byteBuffer.position() == byteBuffer.capacity();
    }

    @Override
    public void recycle() {
        this.readPos = 0;
        this.writePos = 0;
        this.byteBuffer.clear();
        if (sharedBufferPool != null) {
            sharedBufferPool.recycle(byteBuffer);
        }
    }
}
