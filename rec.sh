#!/bin/sh

ssh -t vdukic@bach26 'rm -rf /home/vdukic/development/spark-bench'

rsync -a --exclude '.git' ./ vdukic@bach26:/home/vdukic/development/spark-bench/

