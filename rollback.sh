#!/bin/bash

successflag=$1
echo "Flag is set to:" $successflag


list_hdfs () {
  hdfs dfs -ls /user/cloudera/examples/apps
}

check_dir_exists () {

if hdfs dfs -test -d /user/cloudera/examples/apps/tmp-java ; then
	echo "Directory  exists"
	hdfs dfs -rm -r /user/cloudera/examples/apps/tmp-java
	echo "Deleting directory"
	list_hdfs
else
	hdfs dfs -mkdir /user/cloudera/examples/apps/tmp-java
	echo "Creating  directory"
	list_hdfs
fi

}

create_tmp_folder () {
hdfs dfs -mkdir /user/cloudera/examples/apps/tmp-java
list_hdfs
}

move_from_tmp () {
hdfs dfs -mv /user/cloudera/examples/apps/java-main /user/cloudera/examples/apps/tmp-java
list_hdfs
}

copy_from_local () {
hdfs dfs -copyFromLocal /home/cloudera/examples/apps/java-main /user/cloudera/examples/apps/
list_hdfs
}

echo "Print the directory structure"
list_hdfs

echo "Remove the tmp folder"
check_dir_exists

echo "Create tmp folder"
create_tmp_folder

echo "Copy to tmp location"
move_from_tmp

if [ $successflag -eq 1 ];
then 
	echo "Build Success"
	copy_from_local
else
	echo "Build Failed"
	hdfs dfs -mv /user/cloudera/examples/apps/tmp-java /user/cloudera/examples/apps/java-main
fi



