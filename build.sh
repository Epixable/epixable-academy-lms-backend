#!/bin/bash
set -e

echo "🚀 Cleaning old build..."
rm -rf build
rm -rf package
rm -f deployment.zip

echo "📁 Creating build folders..."
mkdir -p build
mkdir -p package

echo "📦 Installing dependencies (from requirements.txt)..."
python -m pip install --upgrade pip
python -m pip install -r requirements.txt --target ./package

echo "🗜 Zipping dependencies..."
if command -v zip >/dev/null 2>&1; then
    cd package
    zip -r ../build/dependencies.zip .
    cd ..
else
    echo "⚠️ 'zip' command not found. On Windows, use PowerShell Compress-Archive."
fi

echo "🗜 Zipping Lambda code (.py files)..."
if command -v zip >/dev/null 2>&1; then
    zip -r build/code.zip ./*.py
else
    echo "⚠️ 'zip' command not found. On Windows, use PowerShell Compress-Archive."
fi

echo "🧩 Combining into final deployment.zip"
if command -v unzip >/dev/null 2>&1; then
    cd build
    unzip dependencies.zip -d final
    unzip code.zip -d final
    cd final
    zip -r ../../deployment.zip .
    cd ../..
else
    echo "⚠️ 'unzip' command not found. On Windows, manually merge using PowerShell Expand-Archive and Compress-Archive."
fi

echo "🎉 DONE! Upload deployment.zip to Lambda"
