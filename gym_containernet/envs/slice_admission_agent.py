import gym_containernet

from collections import deque
import copy
from datetime import datetime
import numpy as np
import torch
import gym
import random

from parameters import INPUT_DIM, HL1, HL2, OUTPUT_DIM, GAMMA, EPSILON, LEARNING_RATE
from parameters import EPOCHS, MEM_SIZE, BATCH_SIZE, SYNC_FREQ


q_net = torch.nn.Sequential(
    torch.nn.Linear(INPUT_DIM, HL1),
    torch.nn.ReLU(),
    torch.nn.Linear(HL1, HL2),
    torch.nn.ReLU(),
    torch.nn.Linear(HL2, OUTPUT_DIM)
)

target_net = copy.deepcopy(q_net)
target_net.load_state_dict(q_net.state_dict())

loss_fn = torch.nn.MSELoss()
optimizer = torch.optim.Adam(q_net.parameters(), lr=LEARNING_RATE)

losses = []
total_reward_list = []
replay = deque(maxlen=MEM_SIZE)

env = gym.make('slice-admission-v0')

for i in range(1, EPOCHS + 1):
    time = datetime.now().strftime("%d-%m-%Y_%H:%M:%S")
    print(f'\n\n\n{time}\tEpoch {i}:')
    step = 1
    total_reward = 0
    elastic_accepted = 0
    inelastic_accepted = 0
    elastic_rejected = 0
    inelastic_rejected = 0
    state = torch.flatten(torch.from_numpy(env.reset().astype(np.float32))).reshape(1, INPUT_DIM)
    done = False

    while not done:
        print(f"Step {step}")
        step += 1
        qval = q_net(state).data.numpy()
        if not state[0][0]:
            action = 0
        else:
            action = np.random.randint(0, 2) if random.random() < EPSILON else np.argmax(qval)
            if action:
                if int(state[0][0]) == 1:
                    elastic_accepted += 1
                elif int(state[0][0]) == 2:
                    inelastic_accepted += 1
            else:
                if int(state[0][0]) == 1:
                    elastic_rejected += 1
                elif int(state[0][0]) == 2:
                    inelastic_rejected += 1

        next_state, reward, done, _ = env.step(action)
        next_state = torch.flatten(torch.from_numpy(next_state.astype(np.float32))).reshape(1, INPUT_DIM)

        replay.append((state, action, reward, next_state, done))
        state = next_state

        if len(replay) > BATCH_SIZE:
            minibatch = random.sample(replay, BATCH_SIZE)
            state_batch = torch.cat([s1 for (s1, a, r, s2, d) in minibatch])
            action_batch = torch.Tensor([a for (s1, a, r, s2, d) in minibatch])
            reward_batch = torch.Tensor([r for (s1, a, r, s2, d) in minibatch])
            next_state_batch = torch.cat([s2 for (s1, a, r, s2, d) in minibatch])
            done_batch = torch.Tensor([d for (s1, a, r, s2, d) in minibatch])
            Q1 = q_net(state_batch)
            with torch.no_grad():
                Q2 = target_net(next_state_batch)

            Y = reward_batch + GAMMA * ((1 - done_batch) * torch.max(Q2, dim=1)[0])
            X = Q1.gather(dim=1, index=action_batch.long().unsqueeze(dim=1)).squeeze()
            loss = loss_fn(X, Y.detach())
            optimizer.zero_grad()
            loss.backward()
            losses.append(loss.item())
            optimizer.step()

            if step % SYNC_FREQ == 0:
                target_net.load_state_dict(q_net.state_dict())

        total_reward += reward

    if EPSILON > 0.1:
        EPSILON -= (1 / EPOCHS)

    if i % 50 == 0:
        torch.save({
            'epoch': i,
            'epsilon': EPSILON,
            'model_state_dict': q_net.state_dict(),
            'target_state_dict': target_net.state_dict(),
        }, f'models/{time}.pth')

    total_reward_list.append(total_reward)
    print(f"\nEpisode reward: {total_reward}")
    print(f'Accepted:\nElastic: {elastic_accepted}\tInelastic: {inelastic_accepted}\n')
    print(f'Rejected:\nElastic: {elastic_rejected}\tInelastic: {inelastic_rejected}\n')

    with open('rewards.txt', 'a') as results_file:
        results_file.write(f'{total_reward}\n')
    with open('accepted.txt', 'a') as accepted_file:
        accepted_file.write(f'{elastic_accepted}\t{inelastic_accepted}\t'
                            f'{elastic_rejected}\t{inelastic_rejected}\n')
