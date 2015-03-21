#!/bin/bash

rm -rf wc_output
mkdir wc_output

PYTHONPATH=src python -m main
