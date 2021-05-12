# installation default values

def install_dir():
    return '~/deps'

def spark_url():
    return 'https://downloads.apache.org/spark/spark-3.1.1/spark-3.1.1-bin-hadoop2.7.tgz'

def java_url():
    return 'https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz'

def java_min():
    return 11

def java_max():
    return 0

def retries():
    return 5

def use_sudo():
    return False