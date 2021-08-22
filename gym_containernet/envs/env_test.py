import containernet_env
from random import randint

episodes = 50
env = containernet_env.ContainernetEnv()

for episode in range(1, episodes+1):
    print(f"Starting episode {episode}...")
    env.reset()
    done = False
    score = 0

    while not done:
        n_state, reward, done, info = env.step(randint(0, 1))
        print(n_state, reward, done)
        score += reward

    print(f'Episode: {episode}, Score: {score}\n\n')
