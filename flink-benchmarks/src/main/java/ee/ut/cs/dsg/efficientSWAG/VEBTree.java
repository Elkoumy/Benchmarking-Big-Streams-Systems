package ee.ut.cs.dsg.efficientSWAG;

/**
 * The original source code is from
 * https://github.com/umfoida5/vEB
 */

public class VEBTree<V>
{
    public static int BASE_SIZE = 2; /* Base vEB Node size */
    public static int NULL = -1; /* Initial min and max values */
    private VEBNode root;

    /**
     * added by Gamal Elkoumy to calculate the median
     * 12 October 2018
     * University of Tartu
     */
    private int size;
    private int median;

    /**
	 * to store the whole tuple
     */
    private final V NULL_VALUE = (V) new Object();
    private final V[] table;

	/**
     * **********************************************
     *
     */
    /*
     * Creates and returns an instance of a van Emde Boas Tree.
     */
    public static VEBTree createVEBTree(int universeSize)
    {


        if(isPowerOf2(universeSize))
        {
            return new VEBTree(universeSize);
        }
        else
        {
            System.out.println("ERROR: Must create a tree with size a power of 2!");
            return null;
        }
    }

	/**
	 * added by Gamal Elkoumy to calculate the median
     * 12 October 2018
     * University of Tartu
     */
    public V getMedian(){

         return  this.table[ median];

    }

    public String toString(){
//        for ( ee.ut.cs.dsc.efficientSWAG.ee.ut.cs.dsc.efficientSWAG.VEBNode i: root.cluster  ){
//            for (ee.ut.cs.dsc.efficientSWAG.ee.ut.cs.dsc.efficientSWAG.VEBNode j: i.cluster) {
//                System.out.println(j.max);
//                System.out.println(j.min);
//            }
//
//        }
        System.out.println(root.cluster[1].cluster[1].min);
//        System.out.println(root.summary.min);
        return null;
    }
    /*
     * Insert x into the tree.
     */
    public void insert(int x, V item  )
    {
        insertR(root, x);

        /**
         * added by Gamal Elkoumy to calculate the median
         * 12 October 2018
         * University of Tartu
         */
        size+=1;
        if (size==1){
            median = x;
        }else if (size%2==0){
            //even number of elements
            if (x<median){
                // move left
                median = predecessor(median);
            }
        }else {
            // odd number of elements
            if (x > median) {
                // move right
                median = successor(median);
            }
        }

        this.table[x]=item;

    }


    /*
     * Delete x from the tree.
     */
    public void delete(int x)
    {
        deleteR(root, x);

        /**
         * added by Gamal Elkoumy to calculate the median
         * 12 October 2018
         * University of Tartu
         */
        size-=1;

        if (size==0){
            median = NULL;
        }else if (size%2==0){
            //even number of elements
            if (x>median){
                // move left
                median = predecessor(median);
            }
        }else {
            // odd number of elements
            if (x < median) {
                // move right
                median = successor(median);
            }
        }

//        this.table[x]=NULL_VALUE;

    }


    /*
     * Returns true if x is in the tree, false otherwise.
     */
    public boolean search(int x)
    {
        return searchR(root, x);
    }


    /*
     * Returns the predecessor of x, or -1 if x is the minimum.
     */
    public int predecessor(int x)
    {
        int p = predecessorR(root, x);
        return predecessorR(root, x);
    }

    public int successor(int x)
    {
        return successorR(root,x);
    }

    /*
     * Returns the minimum value in the tree or -1 if the tree is empty.
     */
    public int min()
    {
        return root.min;
    }


    /*
     * Returns the maximum value in the tree or -1 if the tree is empty.
     */
    public int max()
    {
        return root.max;
    }


    /*
     * Creates the tree structure with a universe of size
     * universeSize.
     */
    private VEBTree(int universeSize)
    {
        /*
         * This node will handle creating all the other nodes,
         * and the full tree will be built.
         */
        root = new VEBNode(universeSize);
        size =0;
        this.table = (V[]) new Object[universeSize];

    }


    private void insertR(VEBNode node, int x)
    {
        /* This node is empty */
        if(NULL == node.min)
        {
            node.min = x;
            node.max = x;
        }
        if(x < node.min)
        {
            int tempValue = x;
            x = node.min;
            node.min = tempValue;
        }
        if(x > node.min && node.universeSize > BASE_SIZE)
        {
            int highOfX = high(node, x);
            int lowOfX = low(node, x);

            /* Case when the cluster is non-empty*/
            if(NULL != node.cluster[highOfX].min)
            {
                /* Insert into the cluster recursively */
                insertR(node.cluster[highOfX], lowOfX);
            }
            else
            {
                /* Insert into the summary recursively */
                insertR(node.summary, highOfX);
                node.cluster[highOfX].min = lowOfX;
                node.cluster[highOfX].max = lowOfX;
            }
        }
        if(x > node.max)
        {
            node.max = x;
        }
    }


    private void deleteR(VEBNode node, int x)
    {
        if(node.min == node.max)
        {
            node.min = NULL;
            node.max = NULL;
        }
        else if(BASE_SIZE == node.universeSize)
        {
            if(0 == x)
            {
                node.min = 1;
            }
            else
            {
                node.min = 0;
            }
            node.max = node.min;
        }
        else if(x == node.min)
        {
            int summaryMin = node.summary.min;
            x = index(node, summaryMin, node.cluster[summaryMin].min);
            node.min = x;

            int highOfX = high(node, x);
            int lowOfX = low(node, x);
            deleteR(node.cluster[highOfX], lowOfX);

            if(NULL == node.cluster[highOfX].min)
            {
                deleteR(node.summary, highOfX);
                if(x == node.max)
                {
                    int summaryMax = node.summary.max;
                    if(NULL == summaryMax)
                    {
                        node.max = node.min;
                    }
                    else
                    {
                        node.max = index(node, summaryMax, node.cluster[summaryMax].max);
                    }
                }
            }
            else if(x == node.max)
            {
                node.max = index(node, highOfX, node.cluster[highOfX].max);
            }
        }
    }


    private boolean searchR(VEBNode node, int x)
    {
        if(x == node.min || x == node.max)
        {
            return true;
        }
        else if(BASE_SIZE == node.universeSize)
        {
            return false;
        }
        else
        {
            return searchR(node.cluster[high(node, x)], low(node, x));
        }
    }


    /**
     * *********************************************
     */

    public int successorR(VEBNode node, int x)
    {
        if(BASE_SIZE == node.universeSize)
        {
            if(1 == x && 0 == node.max)
            {
                return NULL;
            }
            else
            {
                return 1;
            }
        }
        else if(NULL != node.min && x < node.min)
        {
            return node.min;
        }
        else
        {
            int highOfX = high(node, x);
            int lowOfX = low(node, x);

            int maxCluster = node.cluster[highOfX].max;
            if(NULL != maxCluster && lowOfX < maxCluster)
            {
                return index(node, highOfX, successorR(node.cluster[highOfX], lowOfX));
            }
            else
            {
                int clusterSuc = successorR(node.summary, highOfX);
                if(NULL == clusterSuc)
                {
                    if(NULL != node.max && x < node.max)
                    {
                        return node.max;
                    }
                    else
                    {
                        return NULL;
                    }
                }
                else
                {
                    return index(node, clusterSuc, node.cluster[clusterSuc].min);
                }
            }
        }
    }

    /**
     *
     **********************************************
     */







    public int predecessorR(VEBNode node, int x)
    {
        if(BASE_SIZE == node.universeSize)
        {
            if(1 == x && 0 == node.min)
            {
                return 0;
            }
            else
            {
                return NULL;
            }
        }
        else if(NULL != node.max && x > node.max)
        {
            return node.max;
        }
        else
        {
            int highOfX = high(node, x);
            int lowOfX = low(node, x);

            int minCluster = node.cluster[highOfX].min;
            if(NULL != minCluster && lowOfX > minCluster)
            {
                return index(node, highOfX, predecessorR(node.cluster[highOfX], lowOfX));
            }
            else
            {
                int clusterPred = predecessorR(node.summary, highOfX);
                if(NULL == clusterPred)
                {
                    if(NULL != node.min && x > node.min)
                    {
                        return node.min;
                    }
                    else
                    {
                        return NULL;
                    }
                }
                else
                {
                    return index(node, clusterPred, node.cluster[clusterPred].max);
                }
            }
        }
    }


    /*
     * Returns the integer value of the first half of the bits of x.
     */
    private int high(VEBNode node, int x)
    {
        return (int)Math.floor(x / lowerSquareRoot(node));
    }


    /*
     * Returns the integer value of the second half of the bits of x.
     */
    private int low(VEBNode node, int x)
    {
        return x % (int)lowerSquareRoot(node);
    }


    /*
     * Returns the value of the least significant bits of x.
     */
    private double lowerSquareRoot(VEBNode node)
    {
        /* Change bases to 2 since java api does not support this. */

        return Math.pow(2, Math.floor((Math.log10(node.universeSize) / Math.log10(2)) / 2.0));
    }


    /*
     * Returns the index in the tree of the given value.
     */
    private int index(VEBNode node, int x, int y)
    {
        return (int)(x * lowerSquareRoot(node) + y);
    }


    /*
     * Returns true if x is a power of 2, false otherwise.
     */
    private static boolean isPowerOf2(int x)
    {
        if(0 == x)
        {
            return false;
        }

        while(x % 2 == 0)
        {
            x = x / 2;
        }

        if(x > 1)
        {
            return false;
        }

        return true;
    }
}
