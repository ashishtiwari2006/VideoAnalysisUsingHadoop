# VideoAnalysisUsingHadoop
/*
* Written By : Ashish Kumar Tiwari
* Email: ashishtiwari2006@gmail.com
*/
These code is written for large video analysis using hadoop-map reduce.
It can be break into two part
A. Using Open-CV to get information about how many frames are there in videos and which frames belong to which scene. This information can be written in some text file and using this file convert all the frames of video to big size sequence file with meta information like imageIndex, ScenceIndex etc.. This resulted sequence file later can be used in step B for further analysis using map reduce.
B. Using Hadoop Map Reduce for analysis big sequecne file generated from step one, and can conclude or extract some features like average RGB values for scene (it can even help for commercial detection). Here reduce output is conditional probabilty tables based on theier RGB value this tabel info further can feed in Hidden Markov Model for predition using machine learning.
This Program tested with Ubuntu 14.04 and JDK-8, Hadoop 2.6.0
Required Software to be installed: OpenCV, FFMPEG, Hadoop 
