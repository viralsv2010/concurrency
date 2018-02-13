package concurrency.control;

import concurrency.StorageManager;
import concurrency.control.ConcurrencyController.AbortException;

/**
 * The {@code TimestampConcurrencyControllerTWR} class implements timestamp-based concurrency control with Thomas' write rule.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 *
 * @param <V>
 *            the type of data items
 */
public class TimestampConcurrencyControllerTWR<V> extends TimestampConcurrencyController<V> {

	/**
	 * Constructs a {@code TimestampConcurrencyControllerTWR}.
	 * 
	 * @param storageManager
	 *            a {@code StorageManager}
	 */
	public TimestampConcurrencyControllerTWR(StorageManager<V> storageManager) {
		super(storageManager);
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
		System.out.println("Inside write of TWR");
		int Transaction_Timestamp = tID2timestamp.get(tID);
		System.out.println("Transation Timestamp :: " + Transaction_Timestamp + " For :: " + dID);
		if(dID == 1)
		{
			System.out.println("Write timestamp value for data d" + dID + " is :: " + d1_writeTimestamp );
			if (Transaction_Timestamp < d1_writeTimestamp)
			{
				return;
			}

		}
		
		if(dID == 2)
		{
			System.out.println("Write timestamp value for data d" + dID + " is :: " + d2_writeTimestamp );
			if (Transaction_Timestamp < d2_writeTimestamp)
			{
				return;
			}
		}
		if(dID == 3)
		{
			System.out.println("Write timestamp value for data d" + dID + " is :: " + d3_writeTimestamp );
			if (Transaction_Timestamp < d3_writeTimestamp)
			{
				return;
			}
		}
	

		super.write(tID, dID, dValue);
	}

}
