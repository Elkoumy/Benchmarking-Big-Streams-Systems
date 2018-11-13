package ee.ut.cs.dsg.efficientSWAG;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class STDAggregationFunction implements AggregateFunction<Tuple3<Long,String, Double>, STDAccumulator,Tuple3<Long,String, Double>> {

	public STDAccumulator createAccumulator() {
		return new STDAccumulator();
	}

	public STDAccumulator merge(STDAccumulator a, STDAccumulator b) {
		a.count += b.count;
		a.sum += b.sum;
		a.sum_x2+=b.sum_x2;

		a.std= Math.sqrt( (a.sum_x2/(double)a.count)- (a.sum/(double)a.count)*(a.sum/(double)a.count) );

		return a;
	}

	public STDAccumulator add(Tuple3<Long,String, Double> value, STDAccumulator acc) {
		acc.s= value.f1;
		acc.l= value.f0;
		acc.sum += (double) value.f2;
		acc.count++;
		acc.sum_x2+=value.f2*value.f2;

		acc.std= Math.sqrt( (acc.sum_x2/(double)acc.count)- (acc.sum/(double)acc.count)*(acc.sum/(double)acc.count) );

		return acc;
	}

	public Tuple3<Long,String, Double> getResult(STDAccumulator acc) {
//		return acc.sum / (double) acc.count;
		Tuple3<Long,String, Double> res= new Tuple3<>();
		res.f0=acc.l;
		res.f1= acc.s;
		res.f2 = acc.std;

		return res;
	}
}
