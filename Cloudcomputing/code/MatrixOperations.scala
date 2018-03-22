/**
  * Created by jianingxu on 12/3/16.
  */
package test.finalproject.cloudksvd
import breeze.linalg.{DenseMatrix => BreezeMatrix, DenseVector => BreezeVector}
import org.apache.spark.mllib.linalg.{ DenseVector, Matrix, Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.distributed._

object MatrixOperations{


//read input txt file
  def readFile(s:RDD[String]):RDD[Vector] ={
    val RDD_V = s.map(s => Vectors.dense(s.split(' ').map(_.toDouble)))
    RDD_V

  }


  def getMatrix(v:Matrix):BreezeMatrix[Double]={
    var m=v.numRows
    var n=v.numCols
    new BreezeMatrix[Double](m,n,v.toArray)
  }

  def getVector(v:Vector): BreezeVector[Double] = {
    v match {
      case DenseVector(values) =>
        new BreezeVector[Double](values)
    }
  }
//-----------------Data format transformation--------



//-------------RowMatrix Transpose------------
  def transposeRowMatrix(m: RowMatrix): RowMatrix = {
    val transposedRowsRDD = m.rows.zipWithIndex.map{case (row, rowIndex) => rowToTransposedTriplet(row, rowIndex)}
      .flatMap(x => x) // now we have triplets (newRowIndex, (newColIndex, value))
      .groupByKey
      .sortByKey().map(_._2) // sort rows and remove row indexes
      .map(buildRow) // restore order of elements in each row and remove column indexes
    new RowMatrix(transposedRowsRDD)
  }


  def rowToTransposedTriplet(row: Vector, rowIndex: Long): Array[(Long, (Long, Double))] = {
    val indexedRow = row.toArray.zipWithIndex
    indexedRow.map{case (value, colIndex) => (colIndex.toLong, (rowIndex, value))}
  }

  def buildRow(rowWithIndexes: Iterable[(Long, Double)]): Vector = {
    val resArr = new Array[Double](rowWithIndexes.size)
    rowWithIndexes.foreach{case (index, value) =>
      resArr(index.toInt) = value
    }
    Vectors.dense(resArr)
  }




//-------------Do the RDD multiplication
   def squredRDD (A:RDD[(Long,BreezeMatrix[Double])])={

    var A2=A.map { case (index, e) =>
      var res: Array[BreezeMatrix[Double]] = Array()
      for (i <- 0 until e.cols) {
        res = res :+ e(::, i) * e(::, i).t
      }
      (index, res)
    }
     A2

  }



}
