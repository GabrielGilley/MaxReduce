import matplotlib.pyplot as plt
import numpy as np

# -------------- TXTIME EXTRACTION ------------------
# Strong results 256G
strong8 = [703.306, 714.268, 749.351]
strong16 = [349.789, 354.393, 352.843]
strong32 = [179.886, 185.757, 190.902]
strong64 = [110.773, 110.425, 111.290]

weak8 = [186.104, 186.826, 190.726]
weak16 = [176.098, 184.572, 183.041]
weak32 = [179.886, 185.757, 190.902]
weak64 = [197.196, 196.104, 198.521]


# Lookups
# 3787.169061334338,3877.912259242963,3358.900328692049,1968.3991627520882,2056.9593240539543,2034.355221210979,
# 898.2167022870854,926.2947366116568,931.5848538079299,

strong16 = [3787.169061334338,3877.912259242963,3358.900328692049]
strong32 = [1968.3991627520882,2056.9593240539543,2034.355221210979]
strong64 = [898.2167022870854,926.2947366116568,931.5848538079299]

weak8 = [2072.286378048826,1897.6163072432391,1930.7694562748075]
weak16 = [1608.336669719778,1549.683043668978,1714.4765946790576]
weak32 = [1968.3991627520882,2056.9593240539543,2034.355221210979]
weak64 = [2009.6034795413725,2103.1827536118217,2190.2311235279776]

# -------------- END RESULTS -----------------------

# strongy = [np.mean(strong8), np.mean(strong16), np.mean(strong32), np.mean(strong64)]
weaky = [np.mean(weak8), np.mean(weak16), np.mean(weak32), np.mean(weak64)]

# x = [8, 16, 32, 64]
x = [8, 16, 32, 64]
# strongy = [np.mean(strong16), np.mean(strong32), np.mean(strong64)]

def plot_strong(x, y, name=""):
    plt.figure()
    plt.plot(x, y, marker='o', linestyle='', color='r')
    plt.xscale('log', base=2)
    plt.yscale('log', base=2)
    plt.xticks(x, labels=[str(i) for i in x])
    plt.yticks(y, labels=[str(round(i, 2)) for i in y])
    plt.xlabel("Processors")
    plt.ylabel("Processing Time (s)")
    if name:
        plt.savefig(name + ".png")
    else:
        plt.show()


def plot_weak(x, y, name=""):
    plt.figure()
    plt.plot(x, y, marker='o', linestyle='', color='r')
    plt.xscale('log', base=2)
    plt.xticks(x, labels=[str(i) for i in x])
    plt.ylim(np.min(y) - 200, np.max(y) + 200)
    plt.xlabel("Processors")
    plt.ylabel("Processing Time (s)")
    if name:
        plt.savefig(name + ".png")
    else:
        plt.show()


# plot_strong(x, strongy, "strong_lookup")
plot_weak(x, weaky, "weak_lookup")


