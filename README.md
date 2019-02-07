**I had to remove my miniconda stuff for this to work**
**Getting plasma client object in java running**


1.) Install cmake & libboost stuff. Copy/Paste into terminal:

fedora:
sudo dnf install cmake \
                 qt-devel \
                 boost \
                 boost-devel \
                 boost-filesystem \
                 boost-system \
                 boost-regex \
                 python2-devel

ubuntu: 
sudo apt-get install cmake \
                     libboost-dev \
                     libboost-filesystem-dev \
                     libboost-system-dev \
                     libboost-regex-dev


1.5) have to install Numpy with python-pip for some reason...

pip install Numpy --user


2.) Clone apache arrow repo:

git clone https://github.com/apache/arrow.git


3.) Checkout the tag for the version to build:

cd arrow
git checkout -b origin/master tags/apache-arrow-0.11.0

4.) cd into arrow/cpp and make a release directory:

cd cpp
mkdir release
cd release


5.) run cmake to make another release directory inside this one:

cmake .. -DCMAKE_BUILD_TYPE=Release


6.) cd into arrow/java:

cd ../../java



7.) Run:

mvn clean install -pl plasma -am -Dmaven.test.skip



8.) cd into plasma

cd plasma


9.) run the test script:

./test.sh


10.) Install the libraries

cd ../../cpp/release/release
pip install pyarrow --user
sudo cp -d lib* /usr/lib
sudo cp -d file-to-stream plasma_store_server stream-to-file /usr/bin
sudo /sbin/ldconfig -v
