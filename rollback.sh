#!/bin/bash

 

successflag=$1

echo "Flag is set to:" $successflag

basepath=/user/cloudera/examples/apps

tmppath=/user/cloudera/examples/apps/tmp-java

sourcepath=/user/cloudera/examples/apps/java-main

localpath=/home/cloudera/examples/apps/java-main

 

list_hdfs () {

  hdfs dfs -ls $basepath

}

 

check_dir_exists () {

 

if hdfs dfs -test -d $tmppath ; then

    echo "Directory  exists"

    hdfs dfs -rm -r $tmppath

    echo "Deleting directory"

    list_hdfs

else

    create_tmp_folder

    echo "Creating  directory"

    

fi

 

}

 

create_tmp_folder () {

hdfs dfs -mkdir $tmppath

list_hdfs

}

 

move_from_tmp () {

hdfs dfs -mv $1 $2

list_hdfs

}

 

copy_from_local () {

hdfs dfs -copyFromLocal $localpath $basepath

list_hdfs

}

 

echo "Print the directory structure"

list_hdfs

 

echo "checking directory exists or not"

check_dir_exists

 

echo "Create tmp folder"

create_tmp_folder

 

echo "Copy to tmp location"

move_from_tmp $sourcepath $tmppath

 

if [ $successflag -eq 1 ];

then

    echo "Build Success"

    copy_from_local

else

    echo "Build Failed"

    move_from_tmp $tmppath $sourcepath

fi