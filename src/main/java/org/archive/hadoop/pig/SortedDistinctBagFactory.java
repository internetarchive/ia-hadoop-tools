package org.archive.hadoop.pig;

import java.util.Comparator;
import java.util.List;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.InternalSortedBag;
import org.apache.pig.data.SortedDataBag;
import org.apache.pig.data.Tuple;

public class SortedDistinctBagFactory extends BagFactory {
	
	
	public SortedDistinctBagFactory()
	{
		
	}

    /**
     * Get a default (unordered, not distinct) data bag.
     */
    public DataBag newDefaultBag() {
        DataBag b = new DefaultDataBag();
        registerBag(b);
        return b;
    }
    
    /**
     * Get a default (unordered, not distinct) data bag from
     * an existing list of tuples. Note that the bag does NOT
     * copy the tuples but uses the provided list as its backing store.
     * So it takes ownership of the list.
     */
    public DataBag newDefaultBag(List<Tuple> listOfTuples) {
        DataBag b = new DefaultDataBag(listOfTuples);
        registerBag(b);
        return b;
    }

    /**
     * Get a sorted data bag.
     * @param comp Comparator that controls how the data is sorted.
     * If null, default comparator will be used.
     */
    public DataBag newSortedBag(Comparator<Tuple> comp) {
        DataBag b = new SortedDataBag(comp);
        registerBag(b);
        return b;
    }
    
    /**
     * Get a distinct data bag.
     */
    public DataBag newDistinctBag() {
        //DataBag b = new DistinctDataBag();
    	DataBag b = new InternalSortedBag(3, null);
        registerBag(b);
        return b;
    }
    
}
