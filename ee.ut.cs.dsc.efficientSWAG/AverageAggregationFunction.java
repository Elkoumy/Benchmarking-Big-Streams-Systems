package ee.ut.cs.dsg.efficientSWAG;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;



public class AverageAggregationFunction implements AggregateFunction<Tuple3<Long,String, Double>, AverageAccumulator,Tuple3<Long,String, Double>> {

	public AverageAccumulator createAccumulator() {
		return new AverageAccumulator();
	}

	public AverageAccumulator merge(AverageAccumulator a, AverageAccumulator b) {
		a.count += b.count;
		a.sum += b.sum;
		return a;
	}

	public AverageAccumulator add(Tuple3<Long,String, Double> value, AverageAccumulator acc) {
		acc.s= value.f1;
		acc.l= value.f0;
		acc.sum += (double) value.f2;
		acc.count++;

		return acc;
	}

	public Tuple3<Long,String, Double> getResult(AverageAccumulator acc) {
//		return acc.sum / (double) acc.count;
		Tuple3<Long,String, Double> res= new Tuple3<>();
		res.f0=acc.l;
		res.f1= acc.s;
		res.f2 = acc.sum / (double) acc.count;

		return res;
	}
}
