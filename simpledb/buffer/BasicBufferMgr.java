package simpledb.buffer;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import simpledb.file.*;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {

   private Map<Block, Buffer> bufferPoolMap;
   private int numAvailable;
   private int numBuffers;
   private int clockIndex = 0;
   
   /**
    * Creates a buffer manager having the specified number 
    * of buffer slots.
    * This constructor depends on both the {@link FileMgr} and
    * {@link simpledb.log.LogMgr LogMgr} objects 
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * Those objects are created during system initialization.
    * Thus this constructor cannot be called until 
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    * @param numbuffs the number of buffer slots to allocate
    */
//   BasicBufferMgr(int numbuffs) {
//      bufferpool = new Buffer[numbuffs];
//      numAvailable = numbuffs;
//      for (int i=0; i<numbuffs; i++)
//         bufferpool[i] = new Buffer();
//   }
   
   
   BasicBufferMgr(int numbuffs){
	   bufferPoolMap = new LinkedHashMap<Block, Buffer>();
	   numBuffers= numbuffs;
	   numAvailable = numbuffs;
   }
   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
//   synchronized void flushAll(int txnum) {
//      for (Buffer buff : bufferpool)
//         if (buff.isModifiedBy(txnum))
//         buff.flush();
//   }
   
   synchronized void flushAll(int txnum) {
      for (Entry<Block, Buffer> entry : bufferPoolMap.entrySet()){
    	  Buffer buff = entry.getValue();
          if (buff.isModifiedBy(txnum)){
              buff.flush();	  
          }
      }
   }   
   
   /**
    * Pins a buffer to the specified block. 
    * If there is already a buffer assigned to that block
    * then that buffer is used;  
    * otherwise, an unpinned buffer from the pool is chosen.
    * Returns a null value if there are no available buffers.
    * @param blk a reference to a disk block
    * @return the pinned buffer
    */
//   synchronized Buffer pin(Block blk) {
//      Buffer buff = findExistingBuffer(blk);
//      if (buff == null) {
//         buff = chooseUnpinnedBuffer();
//         if (buff == null)
//            return null;
//         buff.assignToBlock(blk);
//      }
//      if (!buff.isPinned())
//         numAvailable--;
//      buff.pin();
//      return buff;
//   }
   
   synchronized Buffer pin(Block blk) {
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         buff = chooseUnpinnedBuffer();
         if (buff == null)
            return null;
         buff.assignToBlock(blk);
   	     bufferPoolMap.put(blk, buff);
      }
      if (!buff.isPinned())
         numAvailable--;
      buff.pin();
      return buff;
   }
   
   /**
    * Allocates a new block in the specified file, and
    * pins a buffer to it. 
    * Returns null (without allocating the block) if 
    * there are no available buffers.
    * @param filename the name of the file
    * @param fmtr a pageformatter object, used to format the new block
    * @return the pinned buffer
    */
//   synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
//      Buffer buff = chooseUnpinnedBuffer();
//      if (buff == null)
//         return null;
//      buff.assignToNew(filename, fmtr);
//      numAvailable--;
//      buff.pin();
//      return buff;
//   }   
   
   synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
      Buffer buff = chooseUnpinnedBuffer();
      if (buff == null)
         return null;
      buff.assignToNew(filename, fmtr);
      bufferPoolMap.put(buff.block(), buff);
      numAvailable--;
      buff.pin();
      return buff;
   }
   
   /**
    * Unpins the specified buffer.
    * @param buff the buffer to be unpinned
    */
//   synchronized void unpin(Buffer buff) {
//      buff.unpin();
//      if (!buff.isPinned())
//         numAvailable++;
//   }
   
   synchronized void unpin(Buffer buff) {
	      buff.unpin();
	      if (!buff.isPinned()){
	    	  numAvailable++;
	      }
	   }   
   
   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   int available() {
      return numAvailable;
   }
   
//   private Buffer findExistingBuffer(Block blk) {
//  	 System.out.println("Pool Length: " + bufferpool.length  + " Avaliable Size: " + available());
//      for (Buffer buff : bufferpool) {
//         Block b = buff.block();
//         if (b != null && b.equals(blk)){
//            return buff;
//         }
//      }
//      return null;
//   }
   
   private Buffer findExistingBuffer(Block blk) {
	      return bufferPoolMap.get(blk);
   }   
   
//   private Buffer chooseUnpinnedBuffer() {
//      for (Buffer buff : bufferpool)
//         if (!buff.isPinned())
//         return buff;
//      return null;
//   }
      
   private Buffer chooseUnpinnedBuffer() {
      Collection<Buffer> buffers = bufferPoolMap.values();
      int numRotations = 5;
      for(int i = 0; i < numRotations * numBuffers; i++) {
    	  Buffer buffer = buffers.toArray(new Buffer[numBuffers])[clockIndex];//I don't like this. We should find a better way to access the clockIndex'th buffer in the collection returns from bufferPoolMap.values
    	  clockIndex = (clockIndex + 1) % numBuffers;
    	  if(!buffer.isPinned() && buffer.getReferenceCount() == 0) {
    		  bufferPoolMap.remove(buffer.block());
    		  return buffer;
    	  } else if(!buffer.isPinned() && buffer.getReferenceCount() > 0) {
    		  buffer.decrementReferenceCount();
    	  }
      }
      return null;
   }   
  
}
