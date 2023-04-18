#!/bin/sh

URL="https://www.kaggle.com/datasets/ulrikthygepedersen/video-games-sales/download?datasetVersionNumber=1"

LOCAL_PREFIX="data/raw"
LOCAL_ZIP="video-games-sales.zip"
LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_ZIP}"
LOCAL_FILE="video_games_sales.csv"

echo "downloading file to ${LOCAL_PATH}"
echo "--------------------------------------"
mkdir -p ${LOCAL_PREFIX}
cd ${LOCAL_PREFIX}
kaggle datasets download -d ulrikthygepedersen/video-games-sales

echo "unzipping .zip file"
echo "--------------------------------------"
unzip ${LOCAL_ZIP}

echo "removing .zip file"
echo "--------------------------------------"
rm ${LOCAL_ZIP}

echo "compressing to .gz file"
echo "--------------------------------------"
gzip -S ".gz" ${LOCAL_FILE}