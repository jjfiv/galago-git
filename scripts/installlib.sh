#!/bin/bash


if [ -a ../lib/drmaa.jar ] 
then 
    drmaa=../lib/drmaa.jar

elif [ -a ./lib/drmaa.jar ] 
then
    drmaa=./lib/drmaa.jar
fi

if [ -z $drmaa ]
then 
    echo "Can not find drmaa.jar file: it should be in lib directory."
else
    mvn install:install-file -Dfile=$drmaa -Dpackaging=jar -DgroupId=org.ggf.drmaa -Dversion=1.0 -DartifactId=drmaa
fi


