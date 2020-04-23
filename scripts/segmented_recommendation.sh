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
--jars "$additionalJars" \
--class jobs.FrequentlyBoughtJob \
"$artifactLocation" \
--input "$input" \
--output "$output" \
--support "$support" \
--confidence "$confidence" \
--customers "$customers" \
--segment "$segment" \
--recFill "$recFill

/opt/spark/bin/spark-submit --master local[3] \
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
--customers $customers \
--segment $segment \
--recFill $recFill