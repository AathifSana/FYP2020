#!/usr/bin/env bash

for item in $@; do
    case $item in
        (*=*) eval $item;
    esac
done

echo "spark-submit --master local[3] \
--driver-memory "$driverMemory" \
--driver-cores "$driverCores" \
--executor-memory "$executorMemory" \
--executor-cores "$executorCores" \
--class jobs.test.JaccardSimilarityTestJob \
"$artifactLocation" \
--input1 "$input1" \
--input2 "$input2" \
--output "$output

/opt/spark/bin/spark-submit --master local[3] \
--driver-memory $driverMemory \
--driver-cores $driverCores \
--executor-memory $executorMemory \
--executor-cores $executorCores \
--class jobs.test.JaccardSimilarityTestJob \
$artifactLocation \
--input1 $input1 \
--input2 $input2 \
--output $output
