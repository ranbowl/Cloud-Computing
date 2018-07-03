# Cloud-Computing
Cloud K SVD

This project is the scala implementation of cloud k-svd algorithm. cloud K-SVD is
a fast collaborative dictionary learning algorithm for big, distributed data. 

Using sbt to compile the code.

The only parameter is t, which is iteration in k-svd algorithm.

Go to k-svd-scala folder first, and then run following command.

sbt package

spark-submit target/scala-2.10/ksvd_2.10-1.0.jar 10
