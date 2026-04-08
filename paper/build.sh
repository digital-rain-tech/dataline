#!/bin/bash
# Build the paper PDF using XeLaTeX (native Unicode/CJK support)
#
# Requirements:
#   - XeLaTeX (part of TeX Live)
#   - Noto Sans CJK TC font: sudo apt install fonts-noto-cjk
#   - Linux Libertine font: sudo apt install fonts-linuxlibertine
#
# Usage:
#   cd paper && ./build.sh

set -e

echo "Building dataline.pdf with XeLaTeX..."
xelatex -interaction=nonstopmode dataline.tex
bibtex dataline
xelatex -interaction=nonstopmode dataline.tex
xelatex -interaction=nonstopmode dataline.tex

echo ""
echo "Done: dataline.pdf"
ls -lh dataline.pdf
