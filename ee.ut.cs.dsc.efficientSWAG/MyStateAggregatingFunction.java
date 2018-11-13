package ee.ut.cs.dsg.efficientSWAG;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;


public class MyStateAggregatingFunction implements AggregateFunction<Tuple3<Long,String, Double>, MyAverageAccumulator,Tuple3<Long,String, Double>> {

	public MyAverageAccumulator createAccumulator() {
		return new MyAverageAccumulator();
	}

	public MyAverageAccumulator merge(MyAverageAccumulator a, MyAverageAccumulator b) {
		a.count += b.count;
		a.sum += b.sum;
		return a;
	}

	public MyAverageAccumulator add(Tuple3<Long,String, Double> value, MyAverageAccumulator acc) {
		acc.s= value.f1;
		acc.l= value.f0;
		acc.sum += (double) value.f2;
		acc.count++;
//		acc.median.insert(value.f2);
		acc.median_tree.put(value.f2, value);
		return acc;
	}

	public Tuple3<Long,String, Double> getResult(MyAverageAccumulator acc) {
//		return acc.sum / (double) acc.count;
		Tuple3<Long,String, Double> res= new Tuple3<>();
		res.f0=acc.l;
		res.f1= acc.s;
		res.f2 = acc.sum;
//		res.f2=acc.median.median();

		int tree_size= acc.median_tree.size();


		// check odd and even size
//		if( tree_size %2==0) {
//			//even number
//			res.f2 = (double)((double) acc.median_tree.exactKey( (int)tree_size/2-1)+ (double)acc.median_tree.exactKey( (int)tree_size/2))/2.0;
//		}
//		else{
			//odd size
			res.f2 =(double) acc.median_tree.exactKey( tree_size/2);
//		}

		res = (Tuple3<Long,String, Double>) acc.median_tree.get( acc.median_tree.exactKey( tree_size/2));


		return res;
	}

}
