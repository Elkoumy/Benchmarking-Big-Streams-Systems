package ee.ut.cs.dsg.efficientSWAG;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class VEBAggregationFunction implements AggregateFunction<Tuple3<Long,String, Double>, VEBAccumulator,Tuple3<Long,String, Double>> {

	public VEBAccumulator createAccumulator() {
		return new VEBAccumulator();
	}

	public VEBAccumulator merge(VEBAccumulator a,VEBAccumulator b) {
		a.l += b.l;
		a.s += b.s;
		return a;
	}

	public VEBAccumulator add(Tuple3<Long,String, Double> value, VEBAccumulator acc) {
		acc.s= value.f1;
		acc.l= value.f0;
		acc.median.insert((Math.toIntExact( Math.round((double)value.f2))), value);
		return acc;
	}

	public Tuple3<Long,String, Double> getResult(VEBAccumulator acc) {
		Tuple3<Long,String, Double> res= new Tuple3<>();

		res=(Tuple3<Long,String, Double>)acc.median.getMedian();
		return res;
	}

}
