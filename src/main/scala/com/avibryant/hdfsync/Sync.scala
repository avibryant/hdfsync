package com.avibryant.hdfsync

import org.apache.hadoop.util._
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import java.security._
import java.io.FileInputStream
import java.math._

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
      System.err.println("Could not find file: " + src)
      return
    }

    val srcDigest = digest(src)

    val dest = new Path(destDir, srcDigest)
    if(!destFS.exists(dest)) {
      System.err.println(src + " -> " + dest)
      destFS.copyFromLocalFile(false, true, src, dest)
    }
    System.out.println(dest)
  }

  def digest(src : Path) = {
    val md = MessageDigest.getInstance("MD5")
    val is = new FileInputStream(src.toString)
    val buffer = new Array[Byte](8192);
    var read = is.read(buffer)
    while(read > 0) {
      md.update(buffer, 0, read)
      read = is.read(buffer)
    }

    val bigInt = new BigInteger(1, md.digest);
    bigInt.toString(16)
  }
}

object Sync {
  def main(args : Array[String]) {
    ToolRunner.run(new Configuration, new Sync, args)
  }
}
