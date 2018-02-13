package concurrency.control;

import java.util.HashMap;

import concurrency.StorageManager;

/**
 * The {@code TimestampConcurrencyController} class implements timestamp-based concurrency control.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 *
 * @param <V>
 *            the type of data items
 */
public class TimestampConcurrencyController<V> extends ConcurrencyController<V> {

	/**
	 * A map that associates {@code Transaction} IDs with timestamps.
	 */
	HashMap<Integer, Integer> tID2timestamp = new HashMap<Integer, Integer>();
	int R_timestamp = 0;
	int W_timestamp = 0;
	int d1_readTimestamp = 0;
	int d1_writeTimestamp = 0;
	int d2_readTimestamp = 0;
	int d2_writeTimestamp = 0;
	int d3_readTimestamp = 0;
	int d3_writeTimestamp = 0;

	/**
	 * Constructs a {@code TimestampConcurrencyController}.
	 * 
	 * @param storageManager
	 *            a {@code StorageManager}
	 */
	public TimestampConcurrencyController(StorageManager<V> storageManager) {
		super(storageManager);
	}

	/**
	 * Registers a {@code Transaction}.
	 * 
	 * @param tID
	 *            the ID of the {@code Transaction}
	 */
	public void register(int tID) {
		tID2timestamp.put(tID, tID2timestamp.size());
	}

	/**
	 * Handles a read request.
	 * 
	 * @param tID
	 *            the ID of the {@code Transaction} that has made the request
	 * @param dID
	 *            the ID of the data item for which the request was made
	 * @throws InvalidTransactionIDException
	 *             if an invalid {@code Transaction} ID is given
	 * @throws AbortException
	 *             if the request cannot be permitted and thus the related {@code Transaction} must be aborted
	 */
	@Override
	public V read(int tID, int dID) throws InvalidTransactionIDException, AbortException {
		System.out.println("Inside read ::  Transaction ID :: " + tID +" Data :: " + dID );
		if(tID2timestamp.containsKey(tID))
		{
			System.out.println("Transaction Verified.");
			int	Transaction_Timestamp = tID2timestamp.get(tID);
			System.out.println("Transation Timestamp :: " + Transaction_Timestamp + " For :: " + dID);
			if(dID == 1)
			{
				System.out.println("Write timestamp value for data d" + dID + " is :: " + d1_writeTimestamp );
				if (Transaction_Timestamp >= d1_writeTimestamp )
				{
					System.out.println("Before change readtimestamp for 1:: " + d1_readTimestamp);
					R_timestamp = Math.max(d1_readTimestamp, Transaction_Timestamp);
					System.out.println("After change readtimestamp for 1 :: " + d1_readTimestamp);
				}
				else
				{
					throw new AbortException();
				}
			}
			
			if(dID == 2)
			{
				System.out.println("Write timestamp value for data d" + dID  + " is :: " + d2_writeTimestamp );
				if (Transaction_Timestamp >= d2_writeTimestamp )
				{
					System.out.println("Before change readtimestamp for 2 :: " + d2_readTimestamp);
					d2_readTimestamp = Math.max(d2_readTimestamp, Transaction_Timestamp);
					System.out.println("After change readtimestamp for 2 :: " + d2_readTimestamp);
				}
				else
				{
					throw new AbortException();
				}
			}
			if(dID == 3)
			{
				System.out.println("Write timestamp value for data d" + dID + " is :: " + d3_writeTimestamp  );
				if (Transaction_Timestamp >= d3_writeTimestamp )
				{
					System.out.println("Before change readtimestamp for 3 :: " + d3_readTimestamp);
					d3_readTimestamp = Math.max(d3_readTimestamp, Transaction_Timestamp);
					System.out.println("After change readtimestamp for 3 :: " + d3_readTimestamp);
				}
				else
				{
					throw new AbortException();
				}
			}
		}
			return super.read(tID, dID);
		}

	/**
	 * Handles a write request.
	 * 
	 * @param tID
	 *            the ID of the {@code Transaction} that has made the request
	 * @param dID
	 *            the ID of the data item for which the request was made
	 * @param dValue
	 *            the value of the data item for which the request was made
	 * @throws InvalidTransactionIDException
	 *             if an invalid {@code Transaction} ID is given
	 * @throws AbortException
	 *             if the request cannot be permitted and thus the related {@code Transaction} must be aborted
	 */
	@Override
	public void write(int tID, int dID, V dValue) throws InvalidTransactionIDException, AbortException {
		System.out.println("Inside write ::  Transaction ID :: " + tID +" Data :: " + dID );
		if(tID2timestamp.containsKey(tID))
		{
			System.out.println("Transaction Verified.");
			int	Transaction_Timestamp = tID2timestamp.get(tID);
			System.out.println("Transation Timestamp :: " + Transaction_Timestamp + " For :: " + dID);
			if(dID == 1)
			{
				System.out.println("Write timestamp value for data d" + dID + " is :: " + d1_writeTimestamp );
				if (Transaction_Timestamp < d1_writeTimestamp || Transaction_Timestamp < d1_readTimestamp)
				{
					throw new AbortException();
				}
				else
				{
					d1_writeTimestamp = Transaction_Timestamp;
				}
			}
			
			if(dID == 2)
			{
				System.out.println("Write timestamp value for data d" + dID + " is :: " + d2_writeTimestamp );
				if (Transaction_Timestamp < d2_writeTimestamp || Transaction_Timestamp < d2_readTimestamp)
				{
					throw new AbortException();
				}
				else
				{
					d2_writeTimestamp = Transaction_Timestamp;
				}
			}
			if(dID == 3)
			{
				System.out.println("Write timestamp value for data d" + dID + " is :: " + d3_writeTimestamp );
				if (Transaction_Timestamp < d3_writeTimestamp || Transaction_Timestamp < d3_readTimestamp)
				{
					throw new AbortException();
				}
				else
				{
					d3_writeTimestamp = Transaction_Timestamp;
				}
			}
		}
		super.write(tID, dID, dValue);
	}

}
