package simpledb.tx.recovery;



import simpledb.buffer.Buffer;

import simpledb.buffer.BufferMgr;

import simpledb.file.Block;

import simpledb.log.BasicLogRecord;

import simpledb.server.SimpleDB;



/**

 * This was created for task 3

 * txid, the filename and block number of the block being updated, 

 * and the block number in the saved file where the block gets saved

 * @author Joseph Wiggins

 *

 */

public class UpdateLogRecord implements LogRecord {

  //Copied from SetIntRecord.java

  private int txnum, offset;

  private int saveBlockNum; 
  
  private String val;
  
  private Block blk;

   

  /**

    * Copied from SetIntRecord.java

    * Creates a new setint log record.

    * @param txnum the ID of the specified transaction

    * @param blk the block containing the value

    * @param offset the offset of the value in the block

    * @param val the new value

    */

  public UpdateLogRecord(int txnum, Block blk, int offset, int saveBlockNum, String oldVal) {

      this.txnum = txnum;

      this.blk = blk;

      this.offset = offset;

      this.saveBlockNum = saveBlockNum;

      this.val = oldVal;
  }



  /**

    * Copied from SetIntRecord.java

    * Creates a log record by reading five other values from the log.

    * @param rec the basic log record

    */

  public UpdateLogRecord(BasicLogRecord rec) {

      txnum = rec.nextInt();

      String filename = rec.nextString();

      int blknum = rec.nextInt();

      blk = new Block(filename, blknum);

      offset = rec.nextInt();

      saveBlockNum = rec.nextInt();

  }

   

@Override

public int writeToLog() {

	Object[] rec = new Object[] {UPDATE, txnum, blk.fileName(),
        blk.number(), offset, saveBlockNum};

return logMgr.append(rec);

}



@Override

public int op() {

return UPDATE;

}



@Override

public int txNumber() {

return txnum;

}



@Override

public void undo(int txnum) {

BufferMgr buffMgr = SimpleDB.bufferMgr();

Buffer buff = buffMgr.pin(blk);

buff.setString(offset, val, txnum, -1);

buffMgr.unpin(buff);


}



}