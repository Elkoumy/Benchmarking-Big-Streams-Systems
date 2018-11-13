package ee.ut.cs.dsg.efficientSWAG;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class MedianSkipListAggregationFunction implements AggregateFunction<Tuple3<Long,String, Double>, MedianSkipListAccumulator,Tuple3<Long,String, Double>>{
	public MedianSkipListAccumulator createAccumulator() {
		return new MedianSkipListAccumulator();
	}

	public MedianSkipListAccumulator merge(MedianSkipListAccumulator a, MedianSkipListAccumulator b) {
		a.l += b.l;
		a.s += b.s;
		return a;
	}

	public MedianSkipListAccumulator add(Tuple3<Long,String, Double> value, MedianSkipListAccumulator acc) {
		acc.s= value.f1;
		acc.l= value.f0;
		acc.median_list.add(value);
		return acc;
	}

	public Tuple3<Long,String, Double> getResult(MedianSkipListAccumulator acc) {

		Tuple3<Long,String, Double> res= new Tuple3<>();
		res.f0=acc.l;
		res.f1= acc.s;

		res = acc.median_list.median();
//		int tree_size= acc.median_tree.size();
//		res.f2 =(double) acc.median_tree.exactKey( tree_size/2);
//		res = (Tuple3<Long,String, Double>) acc.median_tree.get( acc.median_tree.exactKey( tree_size/2));
		return res;
	}
}


