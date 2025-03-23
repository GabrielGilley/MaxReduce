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

# -------------- END RESULTS -----------------------

strongy = [np.mean(strong8), np.mean(strong16), np.mean(strong32), np.mean(strong64)]
weaky = [np.mean(weak8), np.mean(weak16), np.mean(weak32), np.mean(weak64)]

x = [8, 16, 32, 64]


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
    plt.ylim(np.mean(y) - 200, np.mean(y) + 200)
    plt.xlabel("Processors")
    plt.ylabel("Processing Time (s)")
    if name:
        plt.savefig(name + ".png")
    else:
        plt.show()


plot_strong(x, strongy, "strong_map")
plot_weak(x, weaky, "weak_map")


