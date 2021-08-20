import containernet_env
from random import randint
import time

episodes = 5
env = containernet_env.ContainernetEnv()
time.sleep(5)

for episode in range(1, episodes+1):
    print("Starting episode...")
    env.reset()
    done = False
    score = 0

    while not done:
        n_state, reward, done, info = env.step(randint(0, 1))
        print(n_state, reward, done)
        score += reward

    print(f'Episode: {episode}, Score: {score}\n\n')
