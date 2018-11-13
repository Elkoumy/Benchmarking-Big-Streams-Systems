package ee.ut.cs.dsg.efficientSWAG;

public class MedianRedBlackAccumulator <ACC> {
	String s;
	long l;
	IndexedTreeMap<Double,Object> median_tree = new IndexedTreeMap<Double,Object>();
}
