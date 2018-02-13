package concurrency.control;

import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;

import concurrency.StorageManager;

/**
 * The {@code Strict2PLConcurrencyController} class implements the strict 2 phase-locking protocol.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 *
 * @param <V>
 *            the type of data items
 */
public class Strict2PLConcurrencyController<V> extends ConcurrencyController<V> {

	/**
	 * A map that associates data IDs with locks.
	 */
	HashMap<Integer, ReentrantReadWriteLock> dID2lock = new HashMap<Integer, ReentrantReadWriteLock>();
	ReentrantReadWriteLock d1_lock = new ReentrantReadWriteLock();
	ReentrantReadWriteLock d2_lock = new ReentrantReadWriteLock();
	ReentrantReadWriteLock d3_lock = new ReentrantReadWriteLock();
	/**
	 * Constructs a {@code Strict2PLConcurrencyController}.
	 * 
	 * @param storageManager
	 *            a {@code StorageManager}
	 */
	public Strict2PLConcurrencyController(StorageManager<V> storageManager) {
		super(storageManager);
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
		if(dID2lock.containsKey(dID))
		{
			if(dID == 1)
			{
				if(d1_lock.isWriteLocked()!= true)
				{	
					d1_lock.readLock().lock();
				}
			}
			if(dID == 2)
			{
				if(d2_lock.isWriteLocked()!= true)
				{	
					d2_lock.readLock().lock();
				}			
			}
			if(dID == 3)
			{
				if(d3_lock.isWriteLocked()!= true)
				{	
					d3_lock.readLock().lock();
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
		if(dID2lock.containsKey(dID))
		{
			if(dID == 1)
			{
				if(d1_lock.isWriteLocked()!= true)
				{	
					d1_lock.writeLock().lock();
				}
			}
			if(dID == 2)
			{
				if(d2_lock.isWriteLocked() != true)
				{	
					d2_lock.writeLock().lock();
				}			
			}
			if(dID == 3)
			{
				if(d3_lock.isWriteLocked() != true)
				{	
					d3_lock.writeLock().lock();
				}			
			}
		}
		super.write(tID, dID, dValue);
	}

	/**
	 * Rolls back the specified {@code Transaction}.
	 * 
	 * @param tID
	 *            the ID of the {@code Transaction} to roll back.
	 */
	@Override
	public void rollback(int tID) {
		super.rollback(tID);
		releaseAllRemainingLocks(tID);
	}

	/**
	 * Commits the specified {@code Transaction}.
	 * 
	 * @param tID
	 *            the ID of the {@code Transaction} to commit
	 */
	@Override
	public void commit(int tID) {
		super.commit(tID);
		releaseAllRemainingLocks(tID);
	}

	/**
	 * Releases all remaining {@code Lock}s granted to the specified {@code Transaction}.
	 * 
	 * @param tID
	 *            the ID of the {@code Transaction}
	 */
	protected void releaseAllRemainingLocks(int tID) {
		if(tID == 1)
		{
			if(d1_lock.isWriteLocked() == true)
			{
				d1_lock.readLock().unlock();
				d1_lock.writeLock().unlock();
			}
		}
		if(tID == 2)
		{
			if(d2_lock.isWriteLocked() == true)
			{
				d2_lock.readLock().unlock();
				d2_lock.writeLock().unlock();
			}
		}
		if(tID == 3)
		{
			if(d3_lock.isWriteLocked() == true)
			{
				d3_lock.readLock().unlock();
				d3_lock.writeLock().unlock();
			}
		}
	}

}
