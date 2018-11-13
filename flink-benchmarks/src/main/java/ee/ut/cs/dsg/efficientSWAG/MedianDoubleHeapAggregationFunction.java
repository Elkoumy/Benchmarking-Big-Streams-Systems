package ee.ut.cs.dsg.efficientSWAG;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class MedianDoubleHeapAggregationFunction implements AggregateFunction<Tuple3<Long,String, Double>, MedianDoubleHeapAccumulator,Tuple3<Long,String, Double>> {

	public MedianDoubleHeapAccumulator createAccumulator() {
		return new MedianDoubleHeapAccumulator();
	}

	public MedianDoubleHeapAccumulator merge(MedianDoubleHeapAccumulator a, MedianDoubleHeapAccumulator b) {
		a.l += b.l;
		a.s += b.s;
		return a;
	}

	public MedianDoubleHeapAccumulator add(Tuple3<Long,String, Double> value, MedianDoubleHeapAccumulator acc) {
		acc.s= value.f1;
		acc.l= value.f0;
		acc.median.insert(value.f2);
		return acc;
	}

	public Tuple3<Long,String, Double> getResult(MedianDoubleHeapAccumulator acc) {
		Tuple3<Long,String, Double> res= new Tuple3<>();
		res.f0=acc.l;
		res.f1= acc.s;
		res.f2=acc.median.median();
		return res;
	}

}
