package ee.ut.cs.dsg.efficientSWAG;

import java.util.Comparator;
import java.util.Random;
import org.apache.flink.api.java.tuple.Tuple3;
//0- based indexable skip list.

/**
 * This code from
 * https://github.com/AhmadElsagheer/Competitive-programming-library/blob/master/data_structures/IndexableSkipList.java
 * @param <E>
 */




public class MedianIndexedSkipList  {

	class Node
	{
		Tuple3<Long,String, Double> value;
		int level;
		int length;
		Node next;
		Node down;

		public Node(Tuple3<Long,String, Double> val, int lvl, int ps, Node nxt, Node dwn)
		{
			value = val;
			level = lvl;
			length = ps;
			next = nxt;
			down = dwn;
		}
	}

	final static double p = 0.5;

	Node head;
	Random rand;
	int size;

	public MedianIndexedSkipList()
	{
		head = new Node(null, 0, -1, null, null);
		rand = new Random();
		size = 0;
	}

	public int randomizeLevel()
	{
		int level = 0;
		while (level <= head.level && rand.nextDouble() < p)
			++level;
		return level;
	}

	public Tuple3<Long,String, Double> remove(int index)
	{
		if (index < 0 || index >= size)
			return null;
		Tuple3<Long,String, Double> removed = null;
		int cur_pos = -1;
		Node cur = head;
		while (cur != null)
		{
			if (cur.next != null && cur_pos + cur.length < index)
			{
				cur_pos += cur.length;
				cur = cur.next;
			}
			else
			{
				cur.length--;
				if(cur_pos + cur.length + 1 == index && cur.next != null)
				{
					removed = cur.next.value;
					cur.length += (cur.next.length == -1 ? 0 : cur.next.length);
					cur.next = cur.next.next;
				}
				cur = cur.down;
			}
		}
		--size;
		if(size == 0){
			head = new Node(null, 0, -1, null, null);
		}
		return removed;
	}

	public boolean add(Tuple3<Long,String, Double> val, int index)
	{
		if (index < 0 || index > size)
			return false;

		int level = randomizeLevel();
		if (level > head.level)
			head = new Node(null, level, index, null, head);

		int cur_pos = -1;
		Node cur = head, last = null;
		while(cur != null)
		{
			if (cur.next != null && cur_pos + cur.length < index)
			{
				cur_pos += cur.length;
				cur = cur.next;
			}
			else
			{
				cur.length++;
				if (cur.level <= level)
				{
					Node toAdd = new Node(val, cur.level, 0, cur.next, null);

					toAdd.length = cur.length - (index - cur_pos);
					cur.length = index - cur_pos;

					if(last != null)
						last.down = toAdd;
					cur.next = toAdd;
					last = toAdd;
				}
				cur = cur.down;
			}
		}
		++size;
		return true;
	}

	// for sorted DS
	public boolean add(Tuple3<Long,String, Double> val)
	{
		int index = getIndex(val);
		if (index < 0 || index > size)
			return false;

		int level = randomizeLevel();
		if (level > head.level)
			head = new Node(null, level, index, null, head);

		int cur_pos = -1;
		Node cur = head, last = null;
		while(cur != null)
		{
			if (cur.next != null && cur_pos + cur.length < index)
			{
				cur_pos += cur.length;
				cur = cur.next;
			}
			else
			{
				cur.length++;
				if (cur.level <= level)
				{
					Node toAdd = new Node(val, cur.level, 0, cur.next, null);

					toAdd.length = cur.length - (index - cur_pos);
					cur.length = index - cur_pos;

					if(last != null)
						last.down = toAdd;
					cur.next = toAdd;
					last = toAdd;
				}
				cur = cur.down;
			}
		}
		++size;
		return true;
	}

	public int getIndex(Tuple3<Long,String, Double> val)
	{
		Node cur = head;int cur_pos = -1;
		while(cur != null)
		{
			Node next = cur.next;
			if (next == null || next.value.f2.compareTo(val.f2) > 0)
				cur = cur.down;

			else
			{
				cur_pos += cur.length;
				cur = cur.next;
			}
		}
		return cur_pos + 1;
	}

	public Tuple3<Long,String, Double> get(int index)
	{
		if (index < 0 || index >= size)
			return null;
		Node cur = head;
		int cur_pos = -1;
		while (cur != null)
		{
			if (cur_pos == index)
				return cur.value;
			if (cur.next == null || cur_pos + cur.length > index)
				cur = cur.down;
			else
			{
				cur_pos += cur.length;
				cur = cur.next;
			}
		}
		return null;
	}

	public void print()
	{

		Node first = head;
		while (first != null)
		{
			Node cur = first;
			System.out.print(first.level + " ");
			while (cur != null)
			{
				System.out.print("[" + cur.value + " " + cur.length + "] ");
				cur = cur.next;
			}
			System.out.println();
			first = first.down;
		}
	}

	public Tuple3<Long,String, Double> median(){

		if (this.size%2==0){
			//even
			Tuple3<Long,String, Double> val1=this.get( this.size/2-1);
			Tuple3<Long,String, Double> val2=this.get( this.size/2);
			double avg =( val1.f2+val2.f2)/2;

			Tuple3<Long,String, Double> res= new Tuple3<Long,String, Double>();
			res.f0=val1.f0;
			res.f1=val1.f1;
			res.f2=avg;
			return  res;
		}
		else {
			//odd

			return this.get( this.size/2);
		}



	}


	public static void main(String[] args) {
		MedianIndexedSkipList sl = new MedianIndexedSkipList();
//        int[] data = {4,2,7,0,9,1,3,7,3,4,5,6,0,2,8};

		Tuple3[] data = {new Tuple3(1l,"1",7d),new Tuple3(1l,"1",2d),new Tuple3<Long,String, Double>(5l,"5",5d),new Tuple3<Long,String, Double>(8l,"8",8d),new Tuple3<Long,String, Double>(7l,"7",7d),new Tuple3<Long,String, Double>(3l,"3",3d)};

		//
		for (Tuple3<Long,String, Double> i : data) {
			sl.add(i);
		}



		System.out.println( sl.median() );
	}
}
