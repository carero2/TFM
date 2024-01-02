#!/bin/bash
set -e

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


LOAD_INIT_PARAM=0
INIT_PARAM_FILE=vectors.bin





if hash python 2>/dev/null; then
    PYTHON=python
else
    PYTHON=python3
fi

echo
echo "$ $BUILDDIR/shuffle -memory $MEMORY -verbose $VERBOSE < $COOCCURRENCE_FILE > $COOCCURRENCE_SHUF_FILE"
$BUILDDIR/shuffle -memory $MEMORY -verbose $VERBOSE < $COOCCURRENCE_FILE > $COOCCURRENCE_SHUF_FILE
echo "$ $BUILDDIR/glove -save-file $SAVE_FILE -threads $NUM_THREADS -input-file $COOCCURRENCE_SHUF_FILE -x-max $X_MAX -iter $MAX_ITER -vector-size $VECTOR_SIZE -binary $BINARY -vocab-file $VOCAB_FILE -verbose $VERBOSE -checkpoint-every $CHECKPOINT -seed $SEED -save-init-param $SAVE_INIT_PARAM -load-init-param $LOAD_INIT_PARAM -init-param-file $INIT_PARAM_FILE"
$BUILDDIR/glove -save-file $SAVE_FILE -threads $NUM_THREADS -input-file $COOCCURRENCE_SHUF_FILE -x-max $X_MAX -iter $MAX_ITER -vector-size $VECTOR_SIZE -binary $BINARY -vocab-file $VOCAB_FILE -verbose $VERBOSE -checkpoint-every $CHECKPOINT -seed $SEED -save-init-param $SAVE_INIT_PARAM -load-init-param $LOAD_INIT_PARAM -init-param-file $INIT_PARAM_FILE

