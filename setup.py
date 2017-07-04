#!/usr/bin/env python3
import optparse as op
import os
import shutil
import subprocess
import logging
logger=logging.getLogger(__name__)
#logger.setLevel(logging.INFO)
logging.basicConfig(level=logging.DEBUG
  ,format="%(levelname)s:%(name)s:%(filename)s:%(funcName)s:%(lineno)d: %(message)s")


def parseOptions():
  """Parses command line options
  
  """
  
  parser=op.OptionParser(usage="Usage: %prog"
    ,version="%prog 1.0",description="Sets up Apache Spark")
  
  #parse command line options
  return parser.parse_args()
def replaceStrInFile(strMatch,strReplace,fileName):
  """Replace all occurrences of strMatch with strReplace in file fileName
  """
  
  file=open(fileName,mode='r')
  fileText=file.read()
  file.close()
  fileText=fileText.replace(strMatch,strReplace)
  file=open(fileName,mode='w')
  file.write(fileText)
  file.close()
def appendLineToFile(line,file):
  """Adds the given line to the end of the file
  """
  
  fh=open(file,'a')
  fh.write(line+"\n")
  fh.close()
def runCommand(cmd,verbose=False):
  """Runs the given shell command
  """
  
  process=subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
  stdout,stderr=process.communicate()
  returnCode=process.returncode
  if returnCode!=0 or verbose:
    logger.info("stdout="+str(stdout))
    logger.info("returnCode="+str(returnCode))
    logger.error("stderr="+str(stderr))
def getSparkURL(sparkVersion="2.0.0",hadoopVersion="2.7"):
  """Constructs a spark url from the version of Spark and Hadoop
  """
  
  return "https://d3kbcqa49mib13.cloudfront.net/spark-" \
    +sparkVersion+"-bin-hadoop"+hadoopVersion+".tgz"  
def getSparkFileName(sparkVersion="2.0.0",hadoopVersion="2.7"):
  """Constructs the spark tarball file name from the version of Spark and Hadoop
  """
  
  return "spark-"+sparkVersion+"-bin-hadoop"+hadoopVersion+".tgz"
def downloadSpark(sparkVersion="2.0.0",hadoopVersion="2.7"):
  """Downloads the given version of spark for the given version of hadoop
  """
  
  URL=getSparkURL(sparkVersion,hadoopVersion)
  fileName=getSparkFileName(sparkVersion,hadoopVersion)
  
  import urllib.request
  logger.info("downloading spark from \""+URL+"\" ...")
  urllib.request.urlretrieve(URL,fileName)
  return fileName
def installSpark(sparkFile,dest="/user/local",envFile="/etc/profile.d/spark.sh"):
  """Installs spark from the tarball sparkFile to the given dest directory and
  sets the appropriate environment variables to use spark in the envFile.
  """
  
  runCommand(["tar","-xzf",sparkFile,"-C",dest])
  appendLineToFile("export PYSPARK_PYTHON=python3",envFile)
  appendLineToFile("export PATH="
    +os.path.join(os.path.join(dest,os.path.splitext(sparkFile)[0]),"sbin")
    +":$PATH",envFile)
  appendLineToFile("export PATH="
    +os.path.join(os.path.join(dest,os.path.splitext(sparkFile)[0]),"bin")
    +":$PATH",envFile)
def main():
  
  #parse command line options
  (options,args)=parseOptions()
  
  if len(args)!=1:
    raise Exception("must have exactly one argument, the installation directory")
  
  sparkFile=downloadSpark(sparkVersion="2.1.1")
  installSpark(sparkFile,dest=args[0])
if __name__ == "__main__":
  main()