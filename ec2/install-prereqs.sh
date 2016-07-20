sudo yum -y install gcc-c++ make boost boost-devel pcre pcre-devel doxygen protobuf protobuf-compiler protobuf-devel java-1.7.0-openjdk-devel openssl-devel yum-utils createrepo
echo "[cloudera-cdh5]
# Packages for Cloudera's Distribution for Hadoop, Version 5, on RedHat  or CentOS 6 x86_64
name=Cloudera's Distribution for Hadoop, Version 5
baseurl=https://archive.cloudera.com/cdh5/redhat/6/x86_64/cdh/5/
gpgkey =https://archive.cloudera.com/cdh5/redhat/6/x86_64/cdh/RPM-GPG-KEY-cloudera    
gpgcheck = 1" | sudo tee /etc/yum.repos.d/cloudera-cdh5.repo
sudo yum install zookeeper-native
