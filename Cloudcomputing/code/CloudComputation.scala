/**
  * Created by jianingxu on 12/3/16.
  */

package test.finalproject.cloudksvd

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg._
import test.finalproject.cloudksvd.MatrixOperations._
import breeze.linalg.{DenseMatrix => BreezeMatrix}
import Math.sqrt


object CloudComputation{







//----------Calc the error matrix
  def ErrorMatrix(Y:RDD[Vector] ,D:BreezeMatrix[Double], X: RDD[Vector]):RDD[(Long,BreezeMatrix[Double])]={
    var Yinverse=Y.zipWithIndex()
    var YRDD=Yinverse.map{case (rows, index)=>(index,rows)}
    var Xinverse=X.zipWithIndex()
    var XRDD=Xinverse.map{case (rows, index)=>(index,rows)}

    var Ematrix=YRDD.join(XRDD)
      .map{

        case (index, (row,col))=>
      var E=BreezeMatrix.zeros[Double](D.rows,D.cols)
      var columns=getVector(col)
      for(i<- 0 until D.cols){
        for(j<- 0 until D.cols){
          if(j!=i){
            E(::,i):=E(::,i)+D(::,j)*columns(j)
          }
        }
      }
      var y=getVector(row)
      for(i<-0 until D.cols){
        E(::,i):=y-E(::,i)
      }
      (index, E)
    }
    Ematrix
  }





//----------------Power method to calc Cloud KSVD
  def cloudKSVD(Y:RDD[Vector], D:BreezeMatrix[Double], X: RDD[Vector], Wt:BreezeMatrix[Double] ,Tp:Int, Tc:Int ):BreezeMatrix[Double]={
    var E=ErrorMatrix(Y,D,X)


    var Qtp=E.map{case (index,e)=>
      var q=BreezeMatrix.rand(D.rows,D.cols)
      (index, q)
    }



    var W:BreezeMatrix[Double]=Wt

    for(m<- 0 until Tc-1){
      W=Wt*W
    }

    var Esqure=squredRDD(E)

    for(iter<- 0 until Tp)

    {
      Qtp= Esqure
        .join(Qtp)
        .map{
          case

            (index,(e,q)) =>
        var atomColumn=BreezeMatrix.zeros[Double](D.rows,D.cols)


        for(i<- 0 until q.cols)
        {
          var Z=e(i)*q(::,i)
          var V=Z/W(index.toInt,0)
          var qtp=V/sqrt(V.t * V)
          atomColumn(::,i):=qtp
        }
        (index,atomColumn)
      }

        .flatMap{case (index,q)=>
        var contribution= for(i<- 0 until W.cols) // Consensus averaging to computing accumlation error matrix


          yield{
          (i,q:*W(index.toInt,i))
        }
        contribution
      }.reduceByKey((x1,x2)=>x1+x2).map{case
        (i,res)=>(i.toLong,res)}
    }
    Qtp.map(_._2).first()
  }







  //----------- initial the weight matrix
  def WeightMatrix(n:Int):BreezeMatrix[Double]={

      var W:BreezeMatrix[Double]=BreezeMatrix.zeros[Double](n,n)
      var tmp:BreezeMatrix[Double]=BreezeMatrix.zeros[Double](n,n)
      var r=new scala.util.Random
      for (i<-0 until n){
        var j=i+1
        while(j< n){
          W(i,j)=r.nextInt(2)
          j=j+1

        }
      }
    W
  }
}