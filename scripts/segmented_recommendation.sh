#!/usr/bin/env bash

for item in $@; do
    case $item in
        (*=*) eval $item;
    esac
done

 --segment 1 --recFill /home/aathif/Documents/IIT/L6\ SE/FYP/Project/src/main/resources/recs/gen

echo "spark-submit --master local[3] \
--driver-memory "$driverMemory" \
--driver-cores "$driverCores" \
--executor-memory "$executorMemory" \
--executor-cores "$executorCores" \
--jars "$additionalJars" \
--class jobs.FrequentlyBoughtJob \
"$artifactLocation" \
--input "$input" \
--output "$output" \
--support "$support" \
--confidence "$confidence" \
--inputSeg "$inputSeg" \
--segment "$segment" \
--recFill "$recFill

spark-submit --master local[3] \
--driver-memory $driverMemory \
--driver-cores $driverCores \
--executor-memory $executorMemory \
--executor-cores $executorCores \
--jars $additionalJars \
--class jobs.FrequentlyBoughtJob \
$artifactLocation \
--input $input \
--output $output \
--support $support \
--confidence $confidence \
--inputSeg $inputSeg \
--segment $segment \
--recFill $recFill