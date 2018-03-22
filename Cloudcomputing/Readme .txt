Our project was writtern in scala to run on Spark platform

==============File list===============

1:main.scala: Including the main entrance of whole project, read input data and display output 

2:MatrixOperations.scala: This file including some matrix operations, such as square function and some 

						  format transformation functions, which are neccessary if we want do some calculations on 

						  elementwise.

3:SpareCoding.scala: This file is used for sparse coding stage.

4:CloudComputation.scala: This file is used for main cloud computations, which includes calculating error matrix and 
             				
             			  power method with consensus averaging to achieve cloud computation. 

We use SBT to compile the whole project. The instruction for running on AWS is described in our report.