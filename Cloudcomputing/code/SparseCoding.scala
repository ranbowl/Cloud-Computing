/**
  * Created by jianingxu on 12/3/16.
  */
package test.finalproject.cloudksvd

import collection.mutable.ArrayBuffer



class SparseCoding(dictionary: Array[Array[Double]], lambda: Double, tolerance: Double) {
  /* The objective is to minimize (1/2)||Ax - b||^2 + lambda*|x|_1

   we can do this by coordinate descent. Taking the subgradient w.r.t. x_i and
   setting it to zero we get

   x_i = truncate(A^T(i) dot residual(x_i=0), lambda) / ||A^T(i)||^2

   where residual = b - A dot (x - x_i*e_i)
   (that is, b - A x' where x' is x with the i-th coordinate zeroed out)

   so if we have the current residual b - Ax, to get the residual we need we just
   add x_i A^T_i to it. After updating x_i, we subtract x_i A^T_i.

   Due to efficiency reasons, then, it's much better to work with A^T than with A.
   Hence this really optimizes (1/2)||A^Tx - b||^2 + lambda|x|_1

   We can stop when the change in ||residual|| after going over everything is
   smaller than a given tolerance.
   */

  def dot(v: Array[Double], w: Array[Double]) = {
    var d = 0.0
    var i = 0
    while (i < v.length) {
      d += v(i)*w(i)
      i += 1
    }
    d
  }
  def norm(v: Array[Double]) = dot(v, v)
  val norms = dictionary.map(norm(_)).toArray
  def max(a: Double,  b: Double) = if (a > b) a else b
  def truncate(r: Double) = r.signum * max(0.0, r.abs - lambda)

  def addTo(v: Array[Double], a: Double,  b: Array[Double]) {
    var i = 0
    while (i < v.length) {
      v(i) += a*b(i)
      i += 1
    }
  }


  // In case anyone who comes after me wants to make this faster, obvious approaches are:
  //   1. make x sparse
  //   2. keep track of which coordinates of x change in each iteration, if some don't
  //      change after a couple of iterations just ignore them
  //   3. use BLAS to optimize addTo and dot
  //   4. search more efficiently for which i to update instead of round-robin
  def encode(v: Array[(Int, Double)], verbose: Boolean=false) = {
    val x = Array.ofDim[Double](dictionary.length)
    val residual = Array.ofDim[Double](dictionary(0).length)
    var oldError = Double.PositiveInfinity
    v.foreach(e => residual(e._1) = e._2)
    var error = v.foldLeft(0.0)((sum, e) => sum + e._2*e._2)
    while (oldError - error > tolerance) {
      oldError = error
      var i = 0
      while (i < x.length) {
        addTo(residual, x(i), dictionary(i))
        x(i) = truncate(dot(dictionary(i), residual))/norms(i)
        addTo(residual, -x(i), dictionary(i))
        i += 1
      }
      error = norm(residual)
      if (verbose) println("error: " + error)
    }
    x
  }
}

//-----------The sparse Coding stage
object SparseCoding {
  def sparsecoding(args: Array[String])={
    val rng = new java.util.Random()
    val dict = Array.ofDim[Array[Double]](20)
    dict.zipWithIndex.foreach(ee => {
      val i = ee._2
      dict(i) = Array.ofDim[Double](30)
      dict(i).zipWithIndex.foreach(e => {
        val j = e._2
        dict(i)(j) = rng.nextGaussian()
      })
    })


    val b = new ArrayBuffer[(Int,  Double)]
    b.append((1, -5.0))
    b.append((10, 15.0))
    b.append((7, -1))
    b.append((23, 17.0))
    val coder = new SparseCoding(dict, 0.1, 0.0000001)
    val x = coder.encode(b.toArray)
    x
  }
}