/**
  * Created by jianingxu on 12/3/16.
  */


import org.apache.spark._

import org.apache.spark.rdd.RDD

import org.apache.spark.mllib.linalg._

import test.finalproject.cloudksvd._

import breeze.linalg.{DenseMatrix => BreezeMatrix}

import test.finalproject.cloudksvd.MatrixOperations._

import org.apache.spark.mllib.linalg.distributed._

import test.finalproject.cloudksvd.SparseCoding._



object main{

  def main(args: Array[String]) {




    var t=args(0).toInt

    var tp=args(1).toInt

    var tc=args(2).toInt

    var tol=args(3).toDouble

    var Xt:RDD[Vector]=null

    val conf=new SparkConf().setMaster("local").setAppName("FinalProject")

      .set("spark.ui.port","4040")

    val sc=new SparkContext(conf)
    //-------------- initial setting----------------------

    val InputText=sc.textFile("Input.txt")
    val Input=readFile(InputText).cache();

    //---------------- reading file------------------------

    //Input.collect().foreach(println)

    val rowMatrix=new RowMatrix(Input);

    val AtMatrix=transposeRowMatrix(rowMatrix)

    //AT.collect().foreach(println)











    var n=AtMatrix.numRows().toInt

    var k=Input.count.toInt

    var D=BreezeMatrix.rand(k,k) //initial D dictionary matrix

    var weight=BreezeMatrix.zeros[Double](n,n) //get the random weight matrix

    for(i<- 0 until n){

      for(j<- 0 until n){

        weight(i,j)=1.0/n

      }

    }


//--------------Begin iterative to get the Cloud-KSVD result--------------
    for(i<- 0 until t){

      var D_X=new BreezeMatrix(20,20,sparsecoding(Array("SpareCoding","0.01")))   // Do the local SpareCoding

      D=D_X.reshape(k-1,k-1)


      val columns = D_X.toArray.grouped(D_X.rows)
      val rows = columns.toSeq.transpose
      val vectors = rows.map(row => new DenseVector(row.toArray))


      Xt=sc.parallelize(vectors)



      D=CloudComputation.cloudKSVD(Input,D,Xt,weight,tp,tc)

      val X=transposeRowMatrix(new RowMatrix(Xt))    //transpose X to display

      println(D)

      println(X)

    }



  }

}