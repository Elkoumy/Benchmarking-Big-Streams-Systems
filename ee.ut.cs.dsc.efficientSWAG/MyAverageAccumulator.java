package ee.ut.cs.dsg.efficientSWAG;

public class MyAverageAccumulator <ACC> {
	long count;
	double sum;
	String s;
	long l;
    MedianHeap median = new MedianHeap();
	IndexedTreeMap<Double,Object> median_tree = new IndexedTreeMap<Double,Object>();
}


