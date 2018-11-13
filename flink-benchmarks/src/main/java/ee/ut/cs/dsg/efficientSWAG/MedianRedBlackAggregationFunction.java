package ee.ut.cs.dsg.efficientSWAG;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class MedianRedBlackAggregationFunction implements AggregateFunction<Tuple3<Long,String, Double>, MedianRedBlackAccumulator,Tuple3<Long,String, Double>> {
	public MedianRedBlackAccumulator createAccumulator() {
		return new MedianRedBlackAccumulator();
	}

	public MedianRedBlackAccumulator merge(MedianRedBlackAccumulator a, MedianRedBlackAccumulator b) {
		a.l += b.l;
		a.s += b.s;
		return a;
	}

	public MedianRedBlackAccumulator add(Tuple3<Long,String, Double> value, MedianRedBlackAccumulator acc) {
		acc.s= value.f1;
		acc.l= value.f0;
		acc.median_tree.put(value.f2, value);
		return acc;
	}

	public Tuple3<Long,String, Double> getResult(MedianRedBlackAccumulator acc) {

		Tuple3<Long,String, Double> res= new Tuple3<>();
		res.f0=acc.l;
		res.f1= acc.s;
		int tree_size= acc.median_tree.size();
		res.f2 =(double) acc.median_tree.exactKey( tree_size/2);
		res = (Tuple3<Long,String, Double>) acc.median_tree.get( acc.median_tree.exactKey( tree_size/2));
		return res;
	}

}
