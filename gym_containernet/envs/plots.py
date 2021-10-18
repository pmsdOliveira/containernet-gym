from matplotlib import pylab as plt
from typing import List


def average_rewards(rewards: List[float], size: int) -> None:
    avg = []
    for idx in range(0, len(rewards), size):
        avg += [sum(val for val in rewards[idx:idx + size]) / size]

    plt.figure(figsize=(10, 7))
    plt.plot(avg)
    plt.xlabel(f"Groups of {size} Epochs", fontsize=22)
    plt.ylabel("Average Rewards", fontsize=22)
    plt.savefig(f'avg_rewards_{size}.png')


def average_slices(type: int, elastic: List[int], inelastic: List[int], size: int) -> None:
    avg_elastic, avg_inelastic = [], []
    for idx in range(0, len(elastic), size):
        avg_elastic += [sum(val for val in elastic[idx:idx + size]) / size]
    for idx in range(0, len(inelastic), size):
        avg_inelastic += [sum(val for val in inelastic[idx:idx + size]) / size]

    plt.figure(figsize=(10, 7))
    plt.plot(range(len(avg_elastic)), avg_elastic, 'r')
    plt.plot(range(len(avg_inelastic)), avg_inelastic, 'g')
    plt.xlabel(f"Groups of {size} Epochs", fontsize=22)
    plt.ylabel(f"Average {'Accepted' if type == 1 else 'Rejected'}", fontsize=22)
    plt.savefig(f"avg_{'accepted' if type == 1 else 'rejected'}_accepted_{size}.png")


if __name__ == '__main__':
    with open("rewards.txt", 'r') as f:
        data = [float(val) for val in f.readlines()]

    plt.figure(figsize=(10, 7))
    plt.plot(data)
    plt.xlabel("Epochs", fontsize=22)
    plt.ylabel("Rewards", fontsize=22)
    plt.savefig('rewards.png')

    average_rewards(data, 5)
    average_rewards(data, 10)
    average_rewards(data, 25)
    average_rewards(data, 50)
    average_rewards(data, 100)
    average_rewards(data, 250)
    average_rewards(data, 500)

    elastic_accepted, inelastic_accepted = [], []
    elastic_rejected, inelastic_rejected = [], []
    with open('accepted.txt', 'r') as f:
        lines = f.readlines()
        for line in lines:
            ea, ia, er, ir = line.split()
            elastic_accepted += [int(ea)]
            inelastic_accepted += [int(ia)]
            elastic_rejected += [int(er)]
            inelastic_rejected += [int(ir)]

    plt.figure(figsize=(10, 7))
    plt.plot(range(len(elastic_accepted)), elastic_accepted, 'r')
    plt.plot(range(len(inelastic_accepted)), inelastic_accepted, 'g')
    plt.xlabel("Epochs", fontsize=22)
    plt.ylabel("Accepted", fontsize=22)
    plt.savefig('accepted.png')

    average_slices(1, elastic_accepted, inelastic_accepted, 5)
    average_slices(1, elastic_accepted, inelastic_accepted, 10)
    average_slices(1, elastic_accepted, inelastic_accepted, 25)
    average_slices(1, elastic_accepted, inelastic_accepted, 50)
    average_slices(1, elastic_accepted, inelastic_accepted, 100)
    average_slices(1, elastic_accepted, inelastic_accepted, 250)
    average_slices(1, elastic_accepted, inelastic_accepted, 500)

    plt.figure(figsize=(10, 7))
    plt.plot(range(len(elastic_rejected)), elastic_rejected, 'r')
    plt.plot(range(len(inelastic_rejected)), inelastic_rejected, 'g')
    plt.xlabel("Epochs", fontsize=22)
    plt.ylabel("Rejected", fontsize=22)
    plt.savefig('rejected.png')

    average_slices(0, elastic_rejected, inelastic_rejected, 5)
    average_slices(0, elastic_rejected, inelastic_rejected, 10)
    average_slices(0, elastic_rejected, inelastic_rejected, 25)
    average_slices(0, elastic_rejected, inelastic_rejected, 50)
    average_slices(0, elastic_rejected, inelastic_rejected, 100)
    average_slices(0, elastic_rejected, inelastic_rejected, 250)
    average_slices(0, elastic_rejected, inelastic_rejected, 500)
