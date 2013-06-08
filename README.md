A simple tool for uploading local files to HDFS, that skips files which are already there and haven't been modified.

Build:
````
mvn package
````

Use:
````
bin/hdfsync from_files... src_dir
````