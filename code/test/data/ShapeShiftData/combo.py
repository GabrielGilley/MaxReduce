#Assuming we can compare outputType as a string
#List Amatches, List Bmatches, ListAambiguous, ListBambiguous = findCorr(A,B)
#	sort A by outputType then amount (i.e. higher amounts come first, outputType breaks ties)
#	sort B by outputType then amount
import json

f = open("shapeshift_full_records.json")
the_file = open('somefile.txt', 'a')
ShapeShiftFull = json.load(f)
f2 = open("shapeshift_mixing_transactions.json")
ShapeShiftMix = json.load(f2)
A = []
B = []
Amatches = []
Bmatches = []
ListBambiguous = []
ListAambiguous = []
for i in ShapeShiftFull["data"]:
    A.append(i)
for j in ShapeShiftMix["data"]:
    B.append(j)
#need to sort by amount and then by outputCoin
A = sorted(A, key=lambda k: k['amount'])
B = sorted(B, key=lambda k: k['amount'])
cur1 = cur2 = 0
while cur1 < len(A) and cur2 < len(B):
    acur = A[cur1]
    bcur = B[cur2]
    A_was_ambiguous = False
    B_was_ambiguous = False
    if acur["amount"] > bcur["amount"]:
        cur2+=1
        continue
    if acur["amount"] < bcur["amount"]:
        cur1+=1
        continue
    if acur["curOut"] > bcur["outputType"]:
        cur2+=1
        continue
    if acur["curOut"] < bcur["outputType"]:
        cur1+=1
        continue
			
#//if there's ambiguity get ALL of it out except the last one.  We'll deal with it later.
    while cur2+1 < len(B) and acur["amount"] == B[cur2+1]["amount"] and acur["curOut"] == B[cur2+1]["outputType"]:
        ListBambiguous.append(B[cur2])
        cur2+=1
        B_was_ambiguous = True
    while cur1+1 < len(A) and bcur["amount"] == A[cur1+1]["amount"] and bcur["outputType"] == A[cur1+1]["curOut"]:
        ListAambiguous.append(A[cur1])
        cur1+=1
        A_was_ambiguous = True
			
#		//add the current value of A and B to the respective list
#		//(this is getting the last ambiguous value)
    if B_was_ambiguous or A_was_ambiguous:
        ListBambiguous.append(B[cur2])
        ListAambiguous.append(A[cur1])
    else:
        Bmatches.append(B[cur2])
        Amatches.append(A[cur1])
    cur2+=1
    cur1+=1

j = 0
for i in Bmatches:
    i["timestamp"] = Amatches[j]["timestamp"]
    i["inputType"] = Amatches[j]["curIn"]
    j += 1

the_file = open('somefile.txt', 'a')
for i in Bmatches:
    the_file.write("TAGS\n")
    the_file.write("inputCoin:"+ i["inputType"]+"\n")
    the_file.write("ouputCoin:"+ i["outputType"]+"\n")
    the_file.write("ShapeShift_tx_raw\n")
    the_file.write("VALUE\n")
    the_file.write("inputCoin:"+ i["inputType"]+"\n")
    the_file.write("ouputCoin:"+ i["outputType"]+"\n")
    the_file.write("inputTxId:" + i["txId"] + "\n")
    the_file.write("outputTxId:" + i["withdrawTx"] + "\n")
    the_file.write("timestamp:" + str(i["timestamp"]) + "\n")
    x =  '{:.9f}'.format(i["amount"]) 
    y =  '{:.9f}'.format(float(i["outputCoin"]))
    the_file.write("inputAmount:" + x + "\n")
    the_file.write("outputAmount:" + y + "\n")
    the_file.write("inputAddr:" + i["serviceAddr"] + "\n")
    the_file.write("outputAddr:" + i["withdrawAddr"] + "\n")
    the_file.write("END\n")
