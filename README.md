A simple tool for uploading local files to HDFS, that skips files which are already there and haven't been modified.

Build:
````
mvn package
````

Use:
````
hadoop jar target/hdfsync-0.0.1-jar-with-dependencies.jar from_files... src_dir
````