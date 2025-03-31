#!/bin/bash

# Output two CSV files
# Table 1:
#    USER_ID, PRODUCT_ID, ENGAGEMENT_SCORE
# Table 2:
#    PRODUCT_ID, PRODUCT_SPECS

NUM_PRODUCT_IDS=1000000
PRODUCT_SPEC_MAX=10000
NUM_USERS=1000
PRODUCT_TABLE="product_table.csv"
PRODUCT_TABLE_PANDO="product_table.pando"
USER_TABLE="user_table.csv"
USER_TABLE_PANDO="user_table.pando"

# First, generate a list of product IDs. We'll just make them incrementing ints
shuf -r -i 1-$PRODUCT_SPEC_MAX -n $NUM_PRODUCT_IDS | awk '{print NR "," $0}' > "$PRODUCT_TABLE"

shuf -r -i 1-$NUM_PRODUCT_IDS -n $NUM_USERS | paste -d, - <(shuf -r -i 1-100 -n $NUM_USERS) | awk '{print NR "," $0}' > "$USER_TABLE"

awk '{print "TAGS\nproduct_engagement_table2\nVALUE\n" $0 "\nEND"}' "$PRODUCT_TABLE" > "$PRODUCT_TABLE_PANDO"
awk '{print "TAGS\nproduct_engagement_table1\nVALUE\n" $0 "\nEND"}' "$USER_TABLE" > "$USER_TABLE_PANDO"
