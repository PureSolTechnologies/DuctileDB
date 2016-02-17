#!/bin/sh

# Clean and create build directory
if [ -d "build" ] ; then
   rm -rf build
fi
mkdir build

# Copy the basic elements
cp -rf app build
cp -rf css build
cp -rf fonts build
cp -rf html build
cp -rf images build
cp -rf lib build
cp -rf node_modules build
cp index.html build

# Remove unneeded stuff
rm -rf build/node_modules/lite-server
find build -type d -iname ".*" -exec rm -rf {} \; >> /dev/null
#find build -type d -name "src" -exec rm -rf {} \; >> /dev/null
find build -type f -iname ".*" -exec rm {} \; >> /dev/null
find build -type f -name "*~" -exec rm {} \; >> /dev/null
find build -type f -name "*.ts" -exec rm {} \; >> /dev/null
find build -type f -name "*.js.map" -exec rm {} \; >> /dev/null
find build -type d -iname "example" -exec rm -rf {} \; >> /dev/null
find build -type d -iname "test" -exec rm -rf {} \; >> /dev/null
find build -type d -iname "test" -exec rm -rf {} \; >> /dev/null
find build -type f -iname "*.map" -exec rm {} \; >> /dev/null
find build -type f -name "package.json" -exec rm {} \; >> /dev/null
find build -type f -name "package.json" -exec rm {} \; >> /dev/null
find build -type f -name "package.json" -exec rm {} \; >> /dev/null
find build -type f -name "Gruntfile.json" -exec rm {} \; >> /dev/null
find build -type d -name 'doc' -exec rm -rf {} \; >> /dev/null
cd build && zip -r -9 ../build.zip *
