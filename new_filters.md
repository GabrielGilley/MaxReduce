# The markdown file is organized as follows:
#### 0. [How To: Understanding the new file structure](#0-understanding-the-new-file-structure)
#### A. [How To: Adding filters for a brand new blockchain that isn't a fork](#a-adding-filters-for-a-brand-new-blockchain-that-isnt-a-fork)
#### B. [How To: Adding a new filter for an existing blockchain](#b-adding-a-new-filter-for-an-existing-blockchain)
#### C. [How To: Adding filters for a brand new fork of an existing blockchain](#c-adding-filters-for-a-brand-new-fork-of-an-existing-blockchain)
Note that regardless of which you are doing (A-C), you will still have to manually create test cases, though you can copy the test cases of any pre-existing fork and change the values.

# 0. Understanding the new file structure
On 05/28/24, the main branch was updated with abstracted filter files. The motivation for this was grounded in the fact that many blockchains' filters are easy to make due to there being a one-to-one implementation between blockchains forked from the same blockchain. This process serves to expedite the addition of new forks form existing blockchains.
Note in the example for Section 0, the ZCASH blockchain (ZEC), which is forked from Bitcoin (BTC), is used to demonstrate the file structure.

## What are the hpp files? 
Everything still resides in the same code/filters directory. However, now all filters are abstracted to .hpp files. These files have the naming scheme of ```BTC_BASED_some_filter_name.hpp``` where "BTC" is the crypto tag of the root (original, non-fork) blockchain that is returned by [this function](#b-new-crypto-tag) and "some_filter_name" is the name of the filter as Pando sees it in the FilterInterface. The .hpp files contain all of the code that runs the filters and tells the filters whether or not they should be run. There is an hpp file for each filter for each root blockchain. The functions found in these filters include:
```hpp
fail_() // A function called to add the fail tag to an entry along with the relevant reason
btc_based_should_run_some_filter_name() // This function is called by the filters to determine if the filter should actually be run or not
btc_based_some_filter_name() // This contains all the functionality previously put in the "run" functions. 
```

If we are implementing a filter for bitcoin-based blockchains, we need to create a new fille ```Pando/code/filters/BTC_BASED_some_filter_name.hpp```. This file needs to hold the three functions above. Additionally, utilize other made BTC_BASED_other_filter_name.hpp files to see the structure. Write each function to suit your needs, minimally changing parameters. Some filters don't use the inactive tags, for example, so you may decide those are unnecessary, however, "access, crypto_tag, filter_name, filter_done_tag, and filter_fail_tag" are all required. Also note that the first line of any .hpp function usually involves passing in the ```crypto_tag``` parameter to dbkey.h and getting the key. The ```crypto_key``` returned by [```get_blockchain_key```](#b-new-crypto-tag) is intentionally not passed as a parameter due to its accessibility via the function.

## What are the cpp files for?
Additionally, for each filter for each fork, a .cpp file is created. These files have the naming scheme ```ZEC_some_filter_name.cpp```. Note that the "some_filter_name" in the .cpp and .hpp names as well as the should run and run functions are intended to be the same. Also note that the existence of "BTC_BASED_some_filter_name.hpp does not create a "filter" for BTC, as it just holds the filter code, so you will also need to make ```BTC_some_filter_name.cpp```.

Inside the .cpp files, the relevant (non-boiler-plate) lines of note are:
```cpp
 #include XYZ_BASED_some_filter_name.hpp // Include the relevant filter code
CRYPTO_TAG = "abc"; // This is the tag returned by the inline function in section A.1.b
FILTER = "_some_filter_name"; // This should be the same as the name of the .cpp and .hpp files, as well as the same name in the abc_based functions.

void run_() // This calls the relevant filter function
extern bool should_run() // This calls the relevant should run function
```

# A. Adding filters for a brand new blockchain that isn't a fork 
The best way to make use of this environment, regardless of if there are any available forks of the blockchain you are implementing, is to first implement the blockchain on the highest level, e.g. if you wanted to implement ZCASH, it would be prudent to first implement Bitcoin. Additionally, if you were implementing the highest level already, it would be prudent to implement it as if there were forks of the blockchain, even if there aren't. That means creating a .hpp file for each filter, then creating the subsequent .cpp files for each filter.

## 1. Editing Pando/code/include/dbkey.h
### a. New crypto key
Near the bottom of dbkey.h, in the section: ```/** Blockchain Key Constants, 31 bits **/```, add a key value for the new cryptocurrency, using the next number     in line.
```cpp
 #define ABC_KEY (uint32_t)100
```
### b. New crypto tag
You also must add a crypto *tag*, which is the lexical representation of the crypto currency, which is expected to be the abbreviation of the cryptocurrency it    self, such as bitcoin -> BTC. This tag is used to retrieve the ID. Add the relevant ```else if``` statement to the function below. 

```cpp
inline uint32_t get_blockchain_key (const char* blockchain) {
    // Other cases here
    else if (strcmp(blockchain, "ABC") == 0)
        return ABC_KEY;
    // Default case here
}
```
## 2. Creating the abstract header filters
If we are creating a filter for a new blockchain, we must first make the header files. These follow the pattern of ``` XYZ_BASED_some_filter_name.hpp ```. These files are then called by the .cpp files show in [Section 3](#3-creating-the-cpp-filter-files) below. Reference other .hpp files and [Section 0](#0-understanding-the-new-file-structure) for guidance on creating these files.

## 3. Creating the .cpp filter files
These are the actual files that the FilterInterface recognizes. They simply call the shared code in the hpp files above. Each filter needs its own .cpp file, but now the creation of the cpp files is much simpler. Simply copy another blockchain's .cpp filter files and replace the lines as noted above in [Section 0](#0-understanding-the-new-file-structure).

# B. Adding a new filter for an existing blockchain
This section uses bitcoin as an example. Obviously if you were implementing a filter for ethereum, you would replace BTC -> ETH and bitcoin -> ethereum as needed.
## 1. Find the *already implemented* root blockchain of the blockchain you are adding a filter to
For example, if you are implementing a new filter for ZCASH (ZEC), the blockchain we find in this step is Bitcoin. We want to implement this new filter for all available blockchains, so we will implement it on all bitcoin-based blockchains. If the filter will only work on one particular fork, continue this section as written as that case is handled when it matters.
## 2. Create the .hpp filter
Make the hpp filter in accordance to the guidelines [above](#what-are-the-hpp-files). A new filter for BTC-based blockchains would have the name BTC_BASED_new_filter_name.hpp.
## 3. Create the .cpp filters
Make the cpp filter in accordance to the guidelines [above](#what-are-the-cpp-files-for). For each filter you add, create a cpp file such as BTC_new_filter_name.cpp.
## 4. Include the other blockchains in the root blockchain's fork family (if applicable)
For each blockchain in the fork family that this filter will work on, copy BTC_new_filter_name.cpp to [TAG]_new_filter_name.cpp and change BTC to TAG within the filter, where TAG is a fork of BTC. Perhaps we need a way to manage blockchain families for when there are so many cryptocurrencies implemented that this step becomes a pain?
# C. Adding filters for a brand new fork of an existing blockchain
As stated above, some blockchains are really easy to add filters to due to their one-to-one implementation with other blockchains that they were forked from. For example, ZCASH (ZEC) is forked from Bitcoin (BTC) and therefore they can use the same filters. Bitcoin's forks' filters are found with the file name structure of BTC_BASED_\*.hpp. Pando's interface and the filter names are then abstracted out into their own files, like ZEC_\*.cpp or BTC_\*.cpp, which calls the appropriate .hpp filter. To create filters for a newly implemented fork of another branch, follow the steps below.

We will be using the fake ABC crypto currency, forked from ethereum, as an example. 
## 1. Pando/code/include/dbkey.h
Edit dbkey.h as shown in [Section A.1](#1-editing-pandocodeincludedbkeyh)
 
## 2. Pando/code/filters/
### a. Getting the blockchain parent
i. Determine the original blockchain that the new blockchain was derived from. For example, the Binance Smart Chain might have forks coming off of it, but since the BSC is originally forked from ethereum, ethereum would also be the blockchain we care about when installing filters for BSC's subsequent forks.

ii. Determine the crypto tag of the parent blockchain. Let's say we are implementing "ABC", which is a fork from ethereum, so the parent blockchain's tag is "ETH".

### b. Run the following commands:

#### i. Copy all ETH filters into ABC filters
```bash
for f in ETH_*.cpp; do
    cp "$f" "${f/ETH_/ABC_}"
done
```
#### ii. Change all copied filters to use ABC instead of ETH
```bash
sed -i 's/ETH/ABC/g' ABC_*.cpp
```
That's it! Your filters are completely implemented.


