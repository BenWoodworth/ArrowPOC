#!/usr/bin/env bash

ROOT=`realpath "$(dirname $0)"`
ARROW_DIR="$ROOT/arrow"
ARROW_GIT_REPO="https://github.com/apache/arrow.git"
ARROW_GIT_TAG="apache-arrow-0.12.0"
LOG="$ARROW_DIR/log.txt"
   
set -e
pushd "$ROOT" > /dev/null
    if [[ -d "$ARROW_DIR" ]]
    then
        echo "Directory $ARROW_DIR already exists."
        exit 1
    fi
    
    echo "Creating directory '$ARROW_DIR'."
    mkdir "$ARROW_DIR" > /dev/null
    
    touch "$LOG"
    echo "Logging to '$LOG'."
    
    pushd "$ARROW_DIR" &>> "$LOG"
        echo "Cloning '$ARROW_GIT_REPO'..."
        git clone "https://github.com/apache/arrow.git" ./repo &>> "$LOG"
        
        pushd ./repo > "$LOG"
            echo "Checking out tag '$ARROW_GIT_TAG'..."
            git checkout -b "origin/master" "tags/apache-arrow-0.12.0" &>> "$LOG"
            
            mkdir ./cpp/release &>> "$LOG"
            pushd ./cpp/release &>> "$LOG"
                echo "Building C++ source..."
                cmake .. -DCMAKE_BUILD_TYPE=Release &>> "$LOG"
            popd &>> "$LOG"
            
            pushd ./java &>> "$LOG"
                echo "Building Plasma..."
                mvn clean install -pl plasma -am -Dmaven.test.skip &>> "$LOG"
                
                pushd ./plasma &>> "$LOG"
                    echo "Testing Plasma..."
                    sh ./test.sh &>> "$LOG"
                popd &>> "$LOG"
            popd &>> "$LOG"
        popd &>> "$LOG"
    
        echo "Copying binaries to '$ARROW_DIR/bin'..."
        mkdir ./bin &>> "$LOG"
        cp ./repo/cpp/release/release/* ./bin
        cp ./repo/java/plasma/target/*.jar ./bin

        
        echo "Copying libraries to '$ARROW_DIR/lib'..."
        mkdir ./lib &>> "$LOG"
        mv ./bin/lib* ./lib
    popd &>> "$LOG"
popd > /dev/null

echo "Done!"

