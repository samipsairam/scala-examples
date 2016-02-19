#!/bin/bash

cp=$(find lib_managed -name "*.jar" -exec printf :{} ';'); java -classpath "./target/scala-2.11/simple-project_2.11-1.0.jar:$cp" SimpleApp
