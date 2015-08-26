# VideoAnalysisUsingHadoop
/*
* Written By : Ashish Kumar Tiwari
* Email: ashishtiwari2006@gmail.com
*/


These code is written for large video analysis using Hadoop-map reduce. It can be break into two parts:

A) Using Open-CV to get information about how many frames are there in videos and which frames belong to which scene. This information can be written in some text file and using this file convert all the frames of video to big size sequence file with meta information like image-index, Scene-Index etc.. This resulted sequence file later can be used in step B for further analysis using map reduce.


B) Using Hadoop Map Reduce for analysis big sequence file generated from step one, and can conclude or extract some features like average RGB values for scene (it can even help for commercial detection). Here reduce output is conditional probability tables based on their RGB value this table info further can feed in Hidden Markov Model for prediction using machine learning. This Program tested with Ubuntu 14.04 and JDK-8, Hadoop 2.6.0 Required Software to be installed: OpenCV, FFMPEG, Hadoop 
