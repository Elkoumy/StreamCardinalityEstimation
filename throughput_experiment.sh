#!/bin/bash
# Define a string variable with a value
StringVal="Welcome to linuxhint"
approachs="aggregate scotty"

algorithms="LL AC HLL LC FM HLLP KMV BF"
tps="100000 500000 1000000"
iteration="1"
#distributions="sorted"
distributions="left_skewed sorted normal right_skewed"
#distributions="rightSkewed normal sorted leftSkewed"
distributions="normal"
input_data="/root/swagExperiment/data"
log_dir="/root/swagExperiment/log_dir/"
out_dir="/root/swagExperiment/out_dir/"
# Iterate the string variable using for loop
#for val in $StringVal; do
#    echo $val
#done
for i in $iteration; do
for app in $approachs; do
	echo $app
	for alg in $algorithms; do
		echo $alg
		for t in $tps; do
			echo $t
			for d in $distributions; do
			
/root/swagExperiment/flink-1.7.2/bin/flink run -p 32 -c ee.ut.cs.dsg.StreamCardinality.ThroughputExperiment CardinalityBenchmarking-0.1-SNAPSHOT.jar $alg $app $input_data $t $d
                        java -cp ./GetResults-0.1-SNAPSHOT.jar ee.ut.cs.dsg.ExperimentResults.parsers.GetResults $alg $app $log_dir $t $d		
			
			done
		done
	done	
done
done

python3 ./collecting_results.py $log_dir $out_dir
