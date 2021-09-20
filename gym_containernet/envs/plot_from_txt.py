from matplotlib import pylab as plt


def average(data, size):
    avg = []
    for idx in range(0, len(data), size):
        avg += [sum(val for val in data[idx:idx + size]) / size]

    plt.figure(figsize=(10, 7))
    plt.plot(avg)
    plt.xlabel(f"Groups of {size} Epochs", fontsize=22)
    plt.ylabel("Average Rewards", fontsize=22)
    plt.savefig(f'average_{size}.png')


if __name__ == '__main__':
    with open("results.txt", 'r') as f:
        data = [float(val) for val in f.readlines()]

    plt.figure(figsize=(10, 7))
    plt.plot(data)
    plt.xlabel("Epochs", fontsize=22)
    plt.ylabel("Rewards", fontsize=22)
    plt.savefig('rewards.png')

    average(data, 5)
    average(data, 10)
    average(data, 25)
    average(data, 50)
    average(data, 100)
