package simpledb.buffer;

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
   private Buffer[] bufferPool;
   private int numAvailable;
   private int numBuffers;
   private int numRotations = 5;
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
   
   
   BasicBufferMgr(int numbuffs, int numRotations){
	   bufferPoolMap = new LinkedHashMap<Block, Buffer>();
	   numBuffers= numbuffs;
	   numAvailable = numbuffs;
	   this.numRotations = numRotations;
	   
	   bufferPool = new Buffer[numbuffs];
	   for (int i = 0; i < numbuffs; i++) {
		   bufferPool[i] = new Buffer();
	   }
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
	  System.out.println("Pinning block: " + blk.toString());//TODO
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
      printBufferPoolDetails();
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
	  System.out.println("Pinning New");//TODO
      Buffer buff = chooseUnpinnedBuffer();
      if (buff == null)
         return null;
      buff.assignToNew(filename, fmtr);
      bufferPoolMap.put(buff.block(), buff);
      numAvailable--;
      buff.pin();
      printBufferPoolDetails();
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
	   System.out.println("Unpinning");//TODO
	   buff.unpin();
	   if (!buff.isPinned()){
		   numAvailable++;
	   }
	   printBufferPoolDetails();
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
	  System.out.println("GClock policy used");

	  for(Buffer buffer: bufferPool) {
		  if(buffer.block() == null) {
			  return buffer;
		  }
	  }
	  
      for(int i = 0; i <= numRotations * numBuffers + 1; i++) {
    	  Buffer buffer = bufferPool[clockIndex];
    	  incrementClockIndex();
    	  if(!buffer.isPinned() && buffer.getReferenceCount() == 0) {
    		  bufferPoolMap.remove(buffer.block());
    		  System.out.println(", Block replaced: " + buffer.block().toString());
    		  return buffer;
    	  } else if(!buffer.isPinned() && buffer.getReferenceCount() > 0) {
    		  buffer.decrementReferenceCount();
    	  }
      }
      
      System.out.println("Choose Unpinned Buffer has returned null. This is unexpected.");
      return null;
	  
   }   

   private void incrementClockIndex(){
	   clockIndex = (clockIndex + 1) % numBuffers;
   }
   
   private void printBufferPoolDetails() {
	   StringBuilder sBuilder = new StringBuilder();
	   for(Buffer buffer: bufferPool) {
		   if(buffer.block() != null && bufferPoolMap.containsKey(buffer.block())) {
			   sBuilder.append(buffer.block().toString() + " ");
		   } else {
			   sBuilder.append("- ");
		   }
	   }
	   sBuilder.append("\n");
	   System.out.println(sBuilder.toString());
	   StringBuilder blockPinAndRefCountStringBuilder = new StringBuilder();
	   for(Block block: bufferPoolMap.keySet()) {
		   blockPinAndRefCountStringBuilder.append(block.toString() + ". Pins: " + bufferPoolMap.get(block).getPinCount() + ". Reference count: " + bufferPoolMap.get(block).getReferenceCount() + "\n");
	   }
	   System.out.println(blockPinAndRefCountStringBuilder.toString());
	   System.out.println("Number of buffers available: " + numAvailable);
   }
}
