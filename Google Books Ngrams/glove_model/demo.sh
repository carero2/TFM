#!/bin/bash
set -e

# Makes programs, downloads sample data, trains a GloVe model, and then evaluates it.
# One optional argument can specify the language used for eval script: matlab, octave or [default] python

# make
# if [ ! -e text8 ]; then
#   if hash wget 2>/dev/null; then
#     wget http://mattmahoney.net/dc/text8.zip
#   else
#     curl -O http://mattmahoney.net/dc/text8.zip
#   fi
#   unzip text8.zip
#   rm text8.zip
# fi

CORPUS=text8
VOCAB_FILE=vocab.txt
COOCCURRENCE_FILE=new_cooccurrence.bin
COOCCURRENCE_SHUF_FILE=new_cooccurrence.shuf.bin
BUILDDIR=build
SAVE_FILE=vectors
VERBOSE=0
MEMORY=4.0
VOCAB_MIN_COUNT=100
VECTOR_SIZE=768
MAX_ITER=5
# WINDOW_SIZE=5
BINARY=2
NUM_THREADS=1
X_MAX=100

CHECKPOINT=2

SEED=23


LOAD_INIT_PARAM=1
INIT_PARAM_FILE=vectors.bin





if hash python 2>/dev/null; then
    PYTHON=python
else
    PYTHON=python3
fi

echo
# echo "$ $BUILDDIR/vocab_count -min-count $VOCAB_MIN_COUNT -verbose $VERBOSE < $CORPUS > $VOCAB_FILE"
# $BUILDDIR/vocab_count -min-count $VOCAB_MIN_COUNT -verbose $VERBOSE < $CORPUS > $VOCAB_FILE
# echo "$ $BUILDDIR/cooccur -memory $MEMORY -vocab-file $VOCAB_FILE -verbose $VERBOSE -window-size $WINDOW_SIZE < $CORPUS > $COOCCURRENCE_FILE"
# $BUILDDIR/cooccur -memory $MEMORY -vocab-file $VOCAB_FILE -verbose $VERBOSE -window-size $WINDOW_SIZE < $CORPUS > $COOCCURRENCE_FILE
# echo "$ $BUILDDIR/shuffle -memory $MEMORY -verbose $VERBOSE < $COOCCURRENCE_FILE > $COOCCURRENCE_SHUF_FILE"
# $BUILDDIR/shuffle -memory $MEMORY -verbose $VERBOSE < $COOCCURRENCE_FILE > $COOCCURRENCE_SHUF_FILE
echo "$ $BUILDDIR/glove -save-file $SAVE_FILE -threads $NUM_THREADS -input-file $COOCCURRENCE_SHUF_FILE -x-max $X_MAX -iter $MAX_ITER -vector-size $VECTOR_SIZE -binary $BINARY -vocab-file $VOCAB_FILE -verbose $VERBOSE -checkpoint-every $CHECKPOINT -seed $SEED -save-init-param $SAVE_INIT_PARAM -load-init-param $LOAD_INIT_PARAM -init-param-file $INIT_PARAM_FILE"
$BUILDDIR/glove -save-file $SAVE_FILE -threads $NUM_THREADS -input-file $COOCCURRENCE_SHUF_FILE -x-max $X_MAX -iter $MAX_ITER -vector-size $VECTOR_SIZE -binary $BINARY -vocab-file $VOCAB_FILE -verbose $VERBOSE -checkpoint-every $CHECKPOINT -seed $SEED -save-init-param $SAVE_INIT_PARAM -load-init-param $LOAD_INIT_PARAM -init-param-file $INIT_PARAM_FILE
# if [ "$CORPUS" = 'text8' ]; then
#    if [ "$1" = 'matlab' ]; then
#        matlab -nodisplay -nodesktop -nojvm -nosplash < ./eval/matlab/read_and_evaluate.m 1>&2 
#    elif [ "$1" = 'octave' ]; then
#        octave < ./eval/octave/read_and_evaluate_octave.m 1>&2
#    else
#        echo "$ $PYTHON eval/python/evaluate.py"
#        $PYTHON eval/python/evaluate.py
#    fi
# fi
