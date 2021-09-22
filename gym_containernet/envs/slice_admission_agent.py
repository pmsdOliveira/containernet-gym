import gym_containernet

from collections import deque
import copy
from datetime import datetime
import numpy as np
import torch
import gym
from matplotlib import pylab as plt
import random


l1 = 86
l2 = 150
l3 = 100
l4 = 2

q_net = torch.nn.Sequential(
    torch.nn.Linear(l1, l2),
    torch.nn.ReLU(),
    torch.nn.Linear(l2, l3),
    torch.nn.ReLU(),
    torch.nn.Linear(l3, l4)
)

target_net = copy.deepcopy(q_net)
target_net.load_state_dict(q_net.state_dict())

gamma = 0.9
epsilon = 0.3
learning_rate = 1e-3

loss_fn = torch.nn.MSELoss()
optimizer = torch.optim.Adam(q_net.parameters(), lr=learning_rate)

losses = []
total_reward_list = []
epochs = 5000
mem_size = 1000
batch_size = 200
sync_freq = 500
replay = deque(maxlen=mem_size)

env = gym.make('slice-admission-v0')

for i in range(1, epochs + 1):
    time = datetime.now().strftime("%H:%M:%S")
    print(f'\n\n\n{time}\tEpoch {i}:')
    step = 1
    total_reward = 0
    elastic = 0
    inelastic = 0
    state = torch.flatten(torch.from_numpy(env.reset().astype(np.float32))).reshape(1, 86)
    done = False

    while not done:
        print(f"Step {step}")
        step += 1
        qval = q_net(state).data.numpy()
        if not state[0][0]:
            action = 0
        else:
            action = np.random.randint(0, 2) if random.random() < epsilon else np.argmax(qval)
            if action:
                if int(state[0][0]) == 1:
                    elastic += 1
                elif int(state[0][0]) == 2:
                    inelastic += 1

        next_state, reward, done, _ = env.step(action)
        next_state = torch.flatten(torch.from_numpy(next_state.astype(np.float32))).reshape(1, 86)

        replay.append((state, action, reward, next_state, done))
        state = next_state

        if len(replay) > batch_size:
            minibatch = random.sample(replay, batch_size)
            state_batch = torch.cat([s1 for (s1, a, r, s2, d) in minibatch])
            action_batch = torch.Tensor([a for (s1, a, r, s2, d) in minibatch])
            reward_batch = torch.Tensor([r for (s1, a, r, s2, d) in minibatch])
            next_state_batch = torch.cat([s2 for (s1, a, r, s2, d) in minibatch])
            done_batch = torch.Tensor([d for (s1, a, r, s2, d) in minibatch])
            Q1 = q_net(state_batch)
            with torch.no_grad():
                Q2 = target_net(next_state_batch)

            Y = reward_batch + gamma * ((1 - done_batch) * torch.max(Q2, dim=1)[0])
            X = Q1.gather(dim=1, index=action_batch.long().unsqueeze(dim=1)).squeeze()
            loss = loss_fn(X, Y.detach())
            optimizer.zero_grad()
            loss.backward()
            losses.append(loss.item())
            optimizer.step()

            if step % sync_freq == 0:
                target_net.load_state_dict(q_net.state_dict())

        total_reward += reward

    if epsilon > 0.1:
        epsilon -= (1 / epochs)

    if i % 50 == 0:
        torch.save(q_net.state_dict({
            'epoch': i,
            'epsilon': epsilon,
            'model_state_dict': q_net.state_dict(),
            'target_state_dict': target_net.state_dict(),
        }), f'models/{time}.pth')

    total_reward_list.append(total_reward)
    print(f"\nEpisode reward: {total_reward}")
    print(f'Elastic: {elastic}\tInelastic: {inelastic}')

    with open('rewards.txt', 'a') as results_file:
        results_file.write(f'{total_reward}\n')
    with open('accepted.txt', 'a') as accepted_file:
        accepted_file.write(f'{elastic}\t{inelastic}\n')

print('Plotting losses ...')
plt.figure(figsize=(10, 7))
plt.plot(losses)
plt.xlabel("Epochs", fontsize=22)
plt.ylabel("Loss", fontsize=22)
plt.savefig('avg_loss.png')

print('Plotting rewards ...')
plt.figure(figsize=(10, 7))
plt.plot(total_reward_list)
plt.xlabel("Epochs", fontsize=22)
plt.ylabel("Return", fontsize=22)
plt.savefig('avg_return.png')
