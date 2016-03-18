package org.broadinstitute.hellbender.tools.spark.utils;

import com.google.common.annotations.VisibleForTesting;

import java.io.Serializable;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Immutable hash set.  Provides low memory overhead by using hopscotch hashing.
 * Provides a special iterator type to traverse hash buckets.
 * (This lets you implement a multi-map on this collection by using a <K,V>-ish element type.)
 */
public final class HopscotchHashSet<T> extends AbstractSet<T> implements Serializable {
    private static final long serialVersionUID = 1L;
    private int size;
    private final int capacity;
    // unused buckets contain null.  (this data structure does not support null entries.)
    // if the bucket is unused, the corresponding status byte is irrelevant, but is always set to 0.
    private final T[] buckets;
    // format of the status bytes:
    // high bit set indicates that the bucket contains "chain head" (i.e., an entry that naturally belongs in the
    // corresponding bucket).  high bit not set indicates a "squatter" (i.e., an entry that got placed here through the
    // collision resolution methodology).  we use Byte.MIN_VALUE (i.e., 0x80) to pick off this bit.
    // low 7 bits give the (unsigned) offset from the current entry to the next entry in the collision resolution chain.
    // if the low 7 bits are 0, then we'd be pointing at ourselves, which is nonsense, so that particular value marks
    // "end of chain" instead.  we use Byte.MAX_VALUE (i.e., 0x7f) to pick off these bits.
    private final byte[] status;

    @VisibleForTesting static final double LOAD_FACTOR = .75;

    // largest prime numbers less than increasing powers of 2 (from 8 to 32)
    private static final int[] legalSizes = {
        (1<<8)-5, (1<<9)-3, (1<<10)-3, (1<<11)-9, (1<<12)-3, (1<<13)-1, (1<<14)-3, (1<<15)-19, (1<<16)-15, (1<<17)-1,
        (1<<18)-5, (1<<19)-1, (1<<20)-3, (1<<21)-9, (1<<22)-3, (1<<23)-15, (1<<24)-3, (1<<25)-39, (1<<26)-5,
        (1<<27)-39, (1<<28)-57, (1<<29)-3, (1<<30)-35, Integer.MAX_VALUE
    };

    @SuppressWarnings("unchecked")
    @VisibleForTesting HopscotchHashSet( final int capacity ) {
        this.size = 0;
        this.capacity = capacity;
        // Not unsafe, because the remainder of the API allows only elements known to be T's to be assigned to buckets.
        // In fact, except for the testing interface, this class is an immutable loaded by a collection of T's.
        this.buckets = (T[])new Object[capacity];
        this.status = new byte[capacity];
    }

    public HopscotchHashSet( final Collection<T> entries ) {
        this(computeCapacity(entries.size()));
        for ( final T entry : entries ) {
            if ( entry == null ) throw new UnsupportedOperationException("This type of set can't hold null values.");
            insert(entry);
        }
    }

    @Override
    public boolean contains( final Object value ) {
        if ( value == null ) return false;
        final Iterator<T> itr = bucketIterator(value.hashCode());
        while ( itr.hasNext() ) {
            if ( itr.next().equals(value) ) return true;
        }
        return false;
    }

    @Override
    public int size() { return size; }

    @Override
    public Iterator<T> iterator() { return new CompleteIterator<>(buckets); }

    public Iterator<T> bucketIterator( final int hashVal ) { return new BucketIterator(hashVal); }


    @VisibleForTesting boolean insert( final T entry ) {
        final int bucketIndex = hashToIndex(entry.hashCode());
        // if the place where it should go is empty, just put the new entry there
        if ( buckets[bucketIndex] == null ) {
            emplaceChainHead(bucketIndex, entry);
            size += 1;
            return true;
        }

        // if there's a squatter where the new entry should go, move it elsewhere and put the entry there
        if ( !isChainHead(bucketIndex) ) {
            evict(bucketIndex);
            emplaceChainHead(bucketIndex, entry);
            size += 1;
            return true;
        }

        // find the end of the chain of entries for this bucket, making sure the entry isn't already present
        int endOfChainIndex = bucketIndex;
        while ( true ) {
            // if entry is already in the set
            if ( buckets[endOfChainIndex].equals(entry) ) return false;
            final int offset = getOffset(endOfChainIndex);
            if ( offset == 0 ) break;
            endOfChainIndex = getIndex(endOfChainIndex, offset);
        }

        // find an empty bucket for the new entry
        int emptyBucketIndex = findEmptyBucket(endOfChainIndex);

        // hopscotch the empty bucket into range if it's too far away to point to from endOfChainIndex
        int offset;
        while ( (offset = getIndexDiff(endOfChainIndex, emptyBucketIndex)) > Byte.MAX_VALUE ) {
            emptyBucketIndex = hopscotch(endOfChainIndex, emptyBucketIndex);
        }

        // put the new entry into the empty bucket
        emplaceEndOfChain(emptyBucketIndex, entry);

        // link the current end of chain to the new end of chain
        // (we can add the offset because we know the current offset is zero:  it's a chain end.)
        status[endOfChainIndex] += offset;
        size += 1;
        return true;
    }

    private int hashToIndex( final int hashVal ) {
        return Math.floorMod(hashVal, capacity);
    }

    private void evict( final int bucketToEvictIndex ) {
        int offsetToEvictee = 0;
        int bucketIndex = bucketToEvictIndex;
        while ( ++offsetToEvictee <= Byte.MAX_VALUE ) {
            bucketIndex = getIndex(bucketIndex, -1);
            if ( getOffset(bucketIndex) == offsetToEvictee ) {
                int emptyBucketIndex = findEmptyBucket(bucketToEvictIndex);
                while ( getIndexDiff(bucketIndex, emptyBucketIndex) > Byte.MAX_VALUE ) {
                    //TODO: could be more aggressive.  it might be possible to have the empty bucket between bucketIndex and bucketToEvictIndex
                    emptyBucketIndex = hopscotch(bucketToEvictIndex, emptyBucketIndex);
                }
                move(bucketIndex, bucketToEvictIndex, emptyBucketIndex);
                return;
            }
        }
        throw new IllegalStateException("Can't find predecessor.");
    }

    private void emplaceChainHead( final int bucketIndex, final T entry ) {
        buckets[bucketIndex] = entry;
        status[bucketIndex] = Byte.MIN_VALUE;
    }

    private void emplaceEndOfChain( final int bucketIndex, final T entry ) {
        buckets[bucketIndex] = entry;
        status[bucketIndex] = 0;
    }

    private int findEmptyBucket( int bucketIndex ) {
        if ( size == capacity ) throw new IllegalStateException("HopscotchHashSet is full.");
        do {
            bucketIndex = getIndex(bucketIndex, 1);
        }
        while ( buckets[bucketIndex] != null );
        return bucketIndex;
    }

    private boolean isChainHead( final int bucketIndex ) {
        return (status[bucketIndex] & Byte.MIN_VALUE) != 0;
    }

    private int getOffset( final int bucketIndex ) {
        return status[bucketIndex] & Byte.MAX_VALUE;
    }

    private int getIndex( final int bucketIndex, final int offset ) {
        int result = bucketIndex + offset;
        if ( result >= capacity ) result -= capacity;
        else if ( result < 0 ) result += capacity;
        return result;
    }

    private int getIndexDiff( final int bucketIndex1, final int bucketIndex2 ) {
        int result = bucketIndex2 - bucketIndex1;
        if ( result < 0 ) result += capacity;
        return result;
    }

    private int hopscotch( final int endIndex, final int emptyBucketIndex ) {
        int offsetToEmpty = 0;
        int bucketIndex = emptyBucketIndex;
        while ( ++offsetToEmpty <= Byte.MAX_VALUE ) {
            bucketIndex = getIndex(bucketIndex, -1);
            if ( bucketIndex == endIndex ) break;
            final int offsetInBucket = getOffset(bucketIndex);
            // if the bucketIndex bucket points to something closer than the empty bucket
            if ( offsetInBucket != 0 && offsetInBucket < offsetToEmpty ) {
                final int bucketToMoveIndex = getIndex(bucketIndex, offsetInBucket);
                move( bucketIndex, bucketToMoveIndex, emptyBucketIndex );
                return bucketToMoveIndex;
            }
        }
        throw new IllegalStateException("Hopscotching failed at load factor "+(1.*size/capacity));
    }

    private void move( int predecessorBucketIndex, final int bucketToMoveIndex, final int emptyBucketIndex ) {
        int toEmptyDistance = getIndexDiff(bucketToMoveIndex, emptyBucketIndex);
        int nextOffset = getOffset(bucketToMoveIndex);
        if ( nextOffset == 0 || nextOffset > toEmptyDistance ) {
            status[predecessorBucketIndex] += toEmptyDistance;
        } else {
            status[predecessorBucketIndex] += nextOffset;
            toEmptyDistance -= nextOffset;
            predecessorBucketIndex = getIndex(bucketToMoveIndex, nextOffset);
            while ( (nextOffset = getOffset(predecessorBucketIndex)) != 0 && nextOffset < toEmptyDistance ) {
                toEmptyDistance -= nextOffset;
                predecessorBucketIndex = getIndex(predecessorBucketIndex, nextOffset);
            }
            status[predecessorBucketIndex] = (byte) toEmptyDistance;
        }
        if ( nextOffset != 0 ) {
            status[emptyBucketIndex] = (byte) (nextOffset - toEmptyDistance);
        }
        buckets[emptyBucketIndex] = buckets[bucketToMoveIndex];
        buckets[bucketToMoveIndex] = null;
        status[bucketToMoveIndex] = 0;
    }

    private static int computeCapacity( final int size ) {
        if ( size < LOAD_FACTOR*Integer.MAX_VALUE ) {
            final int augmentedSize = (int) (size / LOAD_FACTOR);
            for ( final int legalSize : legalSizes ) {
                if ( legalSize >= augmentedSize ) return legalSize;
            }
        }
        return legalSizes[legalSizes.length-1];
    }

    private final class BucketIterator implements Iterator<T> {
        private int bucketIndex;

        BucketIterator( final int hashVal ) {
            final int bucketIndex = hashToIndex(hashVal);
            this.bucketIndex = isChainHead(bucketIndex) ? bucketIndex : buckets.length;
        }

        @Override public boolean hasNext() { return bucketIndex != buckets.length; }
        @Override public T next() {
            if ( bucketIndex == buckets.length ) throw new NoSuchElementException("HopscotchHashSet iterator is exhausted.");
            final T result = buckets[bucketIndex];
            final int offset = getOffset(bucketIndex);
            bucketIndex = offset != 0 ? getIndex(bucketIndex, offset) : buckets.length;
            return result;
        }
    }

    private static final class CompleteIterator<T> implements Iterator<T> {
        private final T[] buckets;
        private int bucketIndex;

        CompleteIterator( final T[] buckets ) { this.buckets = buckets; this.bucketIndex = findNext(0); }

        @Override public boolean hasNext() { return bucketIndex != buckets.length; }
        @Override public T next() {
            if ( bucketIndex == buckets.length ) throw new NoSuchElementException("HopscotchHashSet iterator is exhausted.");
            final T result = buckets[bucketIndex];
            bucketIndex = findNext(bucketIndex + 1);
            return result;
        }

        private int findNext( int index ) {
            while ( index < buckets.length && buckets[index] == null ) {
                ++index;
            }
            return index;
        }
    }
}
