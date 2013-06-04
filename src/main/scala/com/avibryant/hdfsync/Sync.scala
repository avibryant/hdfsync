package com.avibryant.hdfsync

import org.apache.hadoop.util._
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

class Sync extends Configured with Tool {
  override def run(args : Array[String]) = {
    if(args.size < 2) {
      System.err.println("Usage: hadoop jar hdfsync.jar src_files... dest_dir")
      1
    }

    val srcFiles = args.init
    val destDir = args.last

    sync(srcFiles, destDir)
  }

  def sync(srcFiles : Seq[String], destDir : String) : Int = {
    val destPath = new Path(destDir)
    val destFS = destPath.getFileSystem(getConf)

    if(!destFS.exists(destPath)) {
      System.err.println("Destination path does not exist: " + destPath)
      return -1
    }

    if(!destFS.isDirectory(destPath)) {
      System.err.println("Destination path is not a directory: " + destPath)
      return -1
    }

    for(src <- srcFiles)
      sync(new Path(src), destPath, destFS)

    0
  }

  def sync(src : Path, destDir : Path, destFS: FileSystem) {
    val srcFS = FileSystem.getLocal(getConf)

    if(!srcFS.exists(src)) {
      System.err.println("Could not find file: " + src);
      return
    }

    val srcStatus = srcFS.getFileStatus(src)
    val dest = new Path(destDir, src.getName)
    if(destFS.exists(dest)) {
      val destStatus = destFS.getFileStatus(dest)
      if(destStatus.getModificationTime < srcStatus.getModificationTime) {
        System.err.println("Updating old file...")
      } else {
          return
      }
    }

    System.err.println(src + " -> " + dest)
    destFS.copyFromLocalFile(false, true, src, dest)
  }
}

object Sync {
  def main(args : Array[String]) {
    ToolRunner.run(new Configuration, new Sync, args)
  }
}