{\rtf1\ansi\ansicpg1252\cocoartf2580
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fswiss\fcharset0 Helvetica;\f1\fswiss\fcharset0 Helvetica-Oblique;}
{\colortbl;\red255\green255\blue255;}
{\*\expandedcolortbl;;}
\paperw11900\paperh16840\margl1440\margr1440\vieww11520\viewh8400\viewkind0
\pard\tx566\tx1133\tx1700\tx2267\tx2834\tx3401\tx3968\tx4535\tx5102\tx5669\tx6236\tx6803\pardirnatural\partightenfactor0

\f0\fs24 \cf0 \
In order to run the project, one should have an Apache spark environment with the right configurations. We use spark 3.2.1.\
\
- 
\f1\i Partintegers.java
\f0\i0  : contains the integer manipulation part implemented with JavaRDDs. It takes 2 arguments, the integer file and an output directory.\
\
- 
\f1\i Partintegers2.java 
\f0\i0 : contains the integer manipulation part implemented with JavaDStream. We decided to use one file that contains the answer to each question. The execution requires to uncomment only the relevant part (delimited between question k and question k+1). It takes 2 arguments, the same as earlier.}