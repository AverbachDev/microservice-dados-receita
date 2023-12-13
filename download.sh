DOWNLOAD_URL=http://200.152.38.155/CNPJ/
DOWNLOAD_DIR=data/download
EXTRACT_DIR=data/output-extract

if [ ! -d $DOWNLOAD_DIR ]; then
    echo "Creating dirs"
    mkdir $DOWNLOAD_DIR $EXTRACT_DIR
fi

if [ -z "$(ls -A $DOWNLOAD_DIR)" ]; then
    echo "Downloading files"
else
    echo "Download directory is not empty, deleting old files and download new files\n"
    rm -R $DOWNLOAD_DIR
    mkdir $DOWNLOAD_DIR
fi

wget --execute="robots = off" --mirror --convert-links --no-parent $DOWNLOAD_URL -A '*.zip' -P $DOWNLOAD_DIR -nd

if [ -z "$(ls -A $EXTRACT_DIR)" ]; then
    echo "Extracting files"
else
    echo "Extraction directory is not empty, deleting old files and extract new files\n"
    rm -R $EXTRACT_DIR
    mkdir $EXTRACT_DIR
fi
    unzip $DOWNLOAD_DIR/\*.zip -d $EXTRACT_DIR

#unzip *.zip -d data/output-extract