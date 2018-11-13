package ee.ut.cs.dsg.efficientSWAG;

import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Scanner;

/**
 *
 * @author BatmanLost
 */
public class MedianHeap {

	//stores all the numbers less than the current median in a maxheap, i.e median is the maximum, at the root
	private PriorityQueue<Double> maxheap;
	//stores all the numbers greater than the current median in a minheap, i.e median is the minimum, at the root
	private PriorityQueue<Double> minheap;

	//comparators for PriorityQueue
	private static final maxHeapComparator myMaxHeapComparator = new maxHeapComparator();
	private static final minHeapComparator myMinHeapComparator = new minHeapComparator();

	/**
	 * Comparator for the minHeap, smallest number has the highest priority, natural ordering
	 */
	private static class minHeapComparator implements Comparator<Double>{
		@Override
		public int compare(Double i, Double j) {
			return i>j ? 1 : i==j ? 0 : -1 ;
		}
	}

	/**
	 * Comparator for the maxHeap, largest number has the highest priority
	 */
	private static  class maxHeapComparator implements Comparator<Double>{
		// opposite to minHeapComparator, invert the return values
		@Override
		public int compare(Double i, Double j) {
			return i>j ? -1 : i==j ? 0 : 1 ;
		}
	}

	/**
	 * Constructor for a ee.ut.cs.dsc.efficientSWAG.ee.ut.cs.dsc.efficientSWAG.MedianHeap, to dynamically generate median.
	 */
	public MedianHeap(){
		// initialize maxheap and minheap with appropriate comparators
		maxheap = new PriorityQueue<Double>(11,myMaxHeapComparator);
		minheap = new PriorityQueue<Double>(11,myMinHeapComparator);
	}

	/**
	 * Returns empty if no median i.e, no input
	 * @return
	 */
	private boolean isEmpty(){
		return maxheap.size() == 0 && minheap.size() == 0 ;
	}

	/**
	 * Inserts into ee.ut.cs.dsc.efficientSWAG.ee.ut.cs.dsc.efficientSWAG.MedianHeap to update the median accordingly
	 * @param n
	 */
	public void insert(double n){
		// initialize if empty
		if(isEmpty()){ minheap.add(n);}
		else{
			//add to the appropriate heap
			// if n is less than or equal to current median, add to maxheap
			if(Double.compare(n, median()) <= 0){maxheap.add(n);}
			// if n is greater than current median, add to min heap
			else{minheap.add(n);}
		}
		// fix the chaos, if any imbalance occurs in the heap sizes
		//i.e, absolute difference of sizes is greater than one.
		fixChaos();
	}

	/**
	 * Re-balances the heap sizes
	 */
	private void fixChaos(){
		//if sizes of heaps differ by 2, then it's a chaos, since median must be the middle element
		if( Math.abs( maxheap.size() - minheap.size()) > 1){
			//check which one is the culprit and take action by kicking out the root from culprit into victim
			if(maxheap.size() > minheap.size()){
				minheap.add(maxheap.poll());
			}
			else{ maxheap.add(minheap.poll());}
		}
	}
	/**
	 * returns the median of the numbers encountered so far
	 * @return
	 */
	public double median(){
		//if total size(no. of elements entered) is even, then median iss the average of the 2 middle elements
		//i.e, average of the root's of the heaps.
		if( maxheap.size() == minheap.size()) {
			return (maxheap.peek() + minheap.peek())/2 ;
		}
		//else median is middle element, i.e, root of the heap with one element more
		else if (maxheap.size() > minheap.size()){ return maxheap.peek();}
		else{ return minheap.peek();}

	}
	/**
	 * String representation of the numbers and median
	 * @return
	 */
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("\n Median for the numbers : " );
		for(double i: maxheap){sb.append(" "+i); }
		for(double i: minheap){sb.append(" "+i); }
		sb.append(" is " + median()+"\n");
		return sb.toString();
	}

	/**
	 * Adds all the array elements and returns the median.
	 * @param array
	 * @return
	 */
	public double addArray(double[] array){
		for(int i=0; i<array.length ;i++){
			insert(array[i]);
		}
		return median();
	}

	/**
	 * Just a test
	 * @param N
	 */
	public void test(int N){
		double[] array = InputGenerator.randomArray(N);
		System.out.println("Input array: \n"+Arrays.toString(array));
		addArray(array);
		System.out.println("Computed Median is :" + median());
		Arrays.sort(array);
		System.out.println("Sorted array: \n"+Arrays.toString(array));
		if(N%2==0){ System.out.println("Calculated Median is :" + (array[N/2] + array[(N/2)-1])/2.0);}
		else{System.out.println("Calculated Median is :" + array[N/2] +"\n");}
	}

	/**
	 * Another testing utility
	 */
	public void printInternal(){
		System.out.println("Less than median, max heap:" + maxheap);
		System.out.println("Greater than median, min heap:" + minheap);
	}

	//Inner class to generate input for basic testing
	private static class InputGenerator {

		public static double[] orderedArray(int N){
			double[] array = new double[N];
			for(int i=0; i<N; i++){
				array[i] = i;
			}
			return array;
		}

		public static double[] randomArray(int N){
			double[] array = new double[N];
			for(int i=0; i<N; i++){
				array[i] = (double)(Math.random()*N*N);
			}
			return array;
		}

		public static double readInt(String s){
			System.out.println(s);
			Scanner sc = new Scanner(System.in);
			return (double) sc.nextInt();
		}
	}


}
