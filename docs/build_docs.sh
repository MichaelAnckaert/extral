#!/bin/bash

# Build script for Extral documentation
# This script builds the HTML documentation using Sphinx

set -e

echo "Building Extral documentation..."

# Navigate to docs directory
cd "$(dirname "$0")"

# Clean previous builds
echo "Cleaning previous builds..."
make clean

# Build HTML documentation
echo "Building HTML documentation..."
make html

echo "Documentation build complete!"
echo "Open docs/_build/html/index.html in your browser to view the documentation."