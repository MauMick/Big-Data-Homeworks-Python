# Big-Data-Homeworks-Python

Homework 1
TRIANGLE COUNTING. In the homework you must implement and test in Spark two MapReduce algorithms to count the number of distinct triangles in an undirected graph G=(V,E), where a triangle is defined by 3 vertices u,v,w in V, such that (u,v),(v,w),(w,u) are in E. In the right image, you find an example of a 5-node graph with 3 triangles. Triangle counting is a popular primitive in social network analysis, where it is used to detect communities and measure the cohesiveness of those communities. It has also been used is different other scenarios, for instance: detecting web spam (the distributions of local triangle frequency of spam hosts significantly differ from those of the non-spam hosts), uncovering the hidden thematic structure in the World Wide Web (connected regions of the web which are dense in triangles represents a common topic), query plan optimization in databases (triangle counting can be used for estimating the size of some joins).

1) Write the method/function MR_ApproxTCwithNodeColors which implements ALGORITHM 1. Specifically, MR_ApproxTCwithNodeColors must take as input an RDD of edges and a number of colors C and must return an estimate tfinal of the number of triangles formed by the input edges computed through transformations of the input RDD, as specified by the algorithm. It is important that the local space required by the algorithm be proportional to the size of the largest subset E(i) (hence, you cannot download the whole graph into a local data structure). Hint: define the hash function hC inside MR_ApproxTCwithNodeColors, but before starting processing the RDD, so that all transformations of the RDD will use the same hash function, but different runs of MR_ApproxTCwithNodeColors will use different hash functions (i.e., defined by different values of a and b).

2) Write the method/function MR_ApproxTCwithSparkPartitions which implements ALGORITHM 2. Specifically, MR_ApproxTCwithSparkPartitions must take as input an RDD of edges and the number of partitions C, and must return an estimate tfinal of the number of triangles formed by the input edges computed through 
 ransformations of the input RDD, as specified by the algorithm. In particular, the input RDD must be subdivided into C partitions and each partition, accessed through one of the mapPartitions methods offered by Spark, will represent one of the subsets E(i) appearing in the high-level description above. If the RDD is passed to the method already subdivided into C partitions, it is not necessary to repartition it.  It is important that the local space required by the algorithm be proportional to the size of the largest subset Spark partition.

3) Write a program which receives in input, as command-line arguments, 2 integers C and R, and a path to the file storing the input graph, and does the following:
Reads parameters C and R
Reads the input graph into an RDD of strings (called rawData) and transform it into an RDD of edges (called edges), represented as pairs of integers, partitioned into C partitions, and cached.
Prints: the name of the file, the number of edges of the graph, C, and R .
Runs R times MR_ApproxTCwithNodeColors to get R independent estimates tfinal of the number of triangles in the input graph.
Prints: the median of the R estimates returned by MR_ApproxTCwithNodeColors and the average running time of MR_ApproxTCwithNodeColors over the R runs.
Runs MR_ApproxTCwithSparkPartitions to get an estimate tfinal of the number of triangles in the input graph.
Prints: the estimate returned by MR_ApproxTCwithSparkPartitions and its running time.

Homework 2
In this homework, you will run a Spark program on the CloudVeneto cluster. As for Homework 1, the objective is to estimate (approximately or exactly) the number of triangles in an undirected graph G=(V,E). More specifically, your program must implement two algorithms:

ALGORITHM 1. The same as Algorithm 1 in Homework 1, so you must recycle method/function MR_ApproxTCwithNodeColors devised for Homework 1, fixing any bugs that we have pointed out to you.

ALGORITHM 2. A 2-round MapReduce algorithm which returns the exact number of triangles. The algorithm is based on node colors (as Algorithm 1) and works as follows. Let C≥1 be the number of colors and let hC(⋅) ne the hash function that assigns a color to each node used in Algorithm 1.

MR_ExactTC must take as input an RDD of edges and the number of colors C, and must return the exact triangle count

The program should
Reads parameters C, R and F
Reads the input graph into an RDD of strings (called rawData) and transform it into an RDD of edges (called edges), represented as pairs of integers, partitioned into 32 partitions, and cached.
Prints: the name of the file, the number of edges of the graph, C, and R
If F=0:
Runs R times MR_ApproxTCwithNodeColors to get R independent estimates of the number of triangles in the input graph.
Prints: the median of the R estimates returned by MR_ApproxTCwithNodeColors and the average running time of MR_ApproxTCwithNodeColors over the R runs.
If F=1:
Runs R times MR_ExactTC to get the exact number of triangles in the input graph
Prints: the last value returned by MR_ExactTC (they are all equal) and the average running time over the R runs.

Homework 3

For the homework, we created a server which generates a continuous stream of integer items. The server has been already activated on the machine algo.dei.unipd.it and emits the items as strings on port 8888. Your program will define a Spark Streaming context that accesses the stream through the method socketTextStream which transforms the input stream (coming from the specified machine and port number) into DStream (Discretized Stream) of batches of items arrived during a time interval whose duration is specified at the creation of the context. A method foreachRDD is then invoked to process the batches one after the other. Each batch is seen as an RDD and a set of RDD methods are available to process it. Typically, the processing of a batch entails the update of some data structures stored in the driver's local space (i.e., its working memory) which are needed to perform the required analysis. The beginning/end of the stream processing will be set by invoking start/stop methods from the context sc. For the homework, the stop command will be invoked after (approximately) 10M items have been read. The threshold 10M will be hardcoded as a constant in the program.

You must write a program GxxxHW3.java (for Java users) or GxxxHW3.py (for Python users), where xxx is your 3-digit group number (e.g., 004 or 045), which receives in input the following 6 command-line arguments (in the given order):

An integer D: the number of rows of the count sketch
An integer W: the number of columns of the count sketch
An integer left: the left endpoint of the interval of interest
An integer right: the right endpoint of the interval of interest
An integer K: the number of top frequent items of interest
An integer portExp: the port number
The program must read the first (approximately) 10M items of the stream Σ generated from machine algo.dei.unipd.it at port portExp and compute the following statistics. Let R denote the interval [left,right] and let ΣR be the substream consisting of all items of Σ belonging to R. The program must compute A D×W count sketch for ΣR. To this purpose, you can use the same family of hash functions used in Homeworks 1 and 2, namely ((ax+b) mod p) mod C, where p=8191, a is a random integer in [1,p-1] and b is a random integer in [0,p-1]. The value C depends on the range you want for the result.
The exact frequencies of all distinct items of ΣR.
The true second moment F2 of ΣR. To avoid large numbers, normalize F2 by dividing it by |ΣR|2.
The approximate second moment F~2 of ΣR using count sketch, also normalized by dividing it by |ΣR|2.
The average relative error of the frequency estimates provided by the count sketch where the average is computed over the items of u∈ΣR whose true frequency is fu≥ϕ(K), where ϕ(K) is the K-th largest frequency of the items of ΣR. Recall that if f~u is the estimated frequency for  u, the relative error of is fu−f~u|/fu.
The program should print:

The input parameters provided as command-line arguments
The lengths of the streams (|Σ| and |ΣR|)
The number of distinct items in ΣR
The average relative error of the frequency estimates for the items of ΣR with the top-K highest true frequencies
(Only if K≤20) True and estimated frequencies of the items of ΣR with the top-K highest true frequencies (no specific order required).
