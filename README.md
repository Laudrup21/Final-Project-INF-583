
In order to run the project, one should have an Apache spark environment with the right configurations. We use spark 3.2.1.
Partintegers.java : contains the integer manipulation part implemented with JavaRDDs. It takes 2 arguments, the integer file and an output directory.\
Partintegers2.java  : contains the integer manipulation part implemented with JavaDStream. We decided to use one file that contains the answer to each question. The execution requires to uncomment only the relevant part (delimited between question k and question k+1). It takes 2 arguments, the same as earlier.}
