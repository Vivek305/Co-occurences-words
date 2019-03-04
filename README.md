# Co-occurences-words

This project was created to implement MapReduce on gigabytes worth of text documents and calculate the co-occurrence
rate between the words in the text file and a specified target word. To optimize parallelization, this project was run
on Amazon EC2 servers as well.

Parameters can be specified in the configuration file, titled conf.xml

###Usage:
Convert your input .txt file into a .seq file that is readable by hadoop  
`make generate-input myinput=YOUR_INPUT_FILE.txt`

You can launch a hadoop job by running the following command:
`$ hadoop jar proj1.jar Proj1 -conf conf.xml <params> <input> <intermediateDir> <outDir>`

The params `<intermediateDir>` and `<outDir>` specify the intermediate and output directories. These can be whatever you like. The intermediate directory will hold the output from the first MapReduce job, which is used as input for the second job. The `<params>` field supports two parameters, `-Dcombiner` and `-DrunJob2`. Setting `-Dcombiner=true` will turn on the combiner, while setting `-Dcombiner=false` will turn it off. Setting `-DrunJob2=false` will cause only the first job to run, and will cause its output to be in a .txt format instead of as sequence files. The default value for -Dcombiner is false and -DrunJob2 is true.

Your output will be found in `<outDir>/part-r-00000`  
It displays the cooccurrence rates in descending order.