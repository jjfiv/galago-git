#!/bin/bash

set -e -u

#
# Dear future me or Galago contributor:
#
# When you decide to give an interesting name to your test,
# please run this script! It will alert you to the fact that
# maven will *only* run tests that are in files with *Test.java 
# at the end.
#

TEST_FILES=`grep "@Test" contrib/src/* core/src/* tupleflow/src/* tupleflow-typebuilder/src/* -Rl`

for FILE_NAME in $TEST_FILES; do
  [[ $FILE_NAME == *Test.java ]] || echo "Maven will skip $FILE_NAME silently! Please rename!"
done

