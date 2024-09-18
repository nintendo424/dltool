#!/bin/bash

for file in inputDats/*.dat; do
    [ -e "$file" ] || continue
    echo $file
    fileName=$(basename "$file")
    fileNameNoExt=${fileName%.*}
    mkdir "outputFiles/$fileNameNoExt"
    cmd="python dltool.py -i \"${file}\" -o \"outputFiles/${fileNameNoExt}\""
    echo "Executing ${cmd}"
    $cmd
done

wait
