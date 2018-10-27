#!/bin/bash

for f in fig/*.svg; do
  inkscape --without-gui --export-pdf="fig/$(basename $f .svg).pdf" $f
done