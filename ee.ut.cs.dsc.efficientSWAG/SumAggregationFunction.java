package ee.ut.cs.dsg.efficientSWAG;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class SumAggregationFunction implements AggregateFunction<Tuple3<Long,String, Double>, SumAccumulator,Tuple3<Long,String, Double>> {

	public SumAccumulator createAccumulator() {
		return new SumAccumulator();
	}

	public SumAccumulator merge(SumAccumulator a, SumAccumulator b) {
		a.count += b.count;
		a.sum += b.sum;
		return a;
	}

	public SumAccumulator add(Tuple3<Long,String, Double> value, SumAccumulator acc) {
		acc.s= value.f1;
		acc.l= value.f0;
		acc.sum += (double) value.f2;
		acc.count++;

		return acc;
	}

	public Tuple3<Long,String, Double> getResult(SumAccumulator acc) {
		Tuple3<Long,String, Double> res= new Tuple3<>();
		res.f0=acc.l;
		res.f1= acc.s;
		res.f2 = acc.sum;


		return res;
	}

}
