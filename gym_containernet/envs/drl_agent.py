import gym_containernet
import matplotlib.pyplot as plt
import tensorflow as tf

from tf_agents.agents.dqn import dqn_agent
from tf_agents.environments import suite_gym
from tf_agents.environments import tf_py_environment
from tf_agents.networks import sequential
from tf_agents.policies import random_tf_policy
from tf_agents.replay_buffers import tf_uniform_replay_buffer
from tf_agents.trajectories import trajectory
from tf_agents.specs import tensor_spec
from tf_agents.utils import common


def dense_layer(num_units):
    return tf.keras.layers.Dense(
        num_units,
        activation=tf.keras.activations.relu,
        kernel_initializer=tf.keras.initializers.VarianceScaling(
            scale=2.0, mode='fan_in', distribution='truncated_normal'))


def build_agent(fc_layer_params, num_actions):
    flatten_layer = tf.keras.layers.Flatten()
    dense_layers = [dense_layer(num_units) for num_units in fc_layer_params]
    q_values_layer = tf.keras.layers.Dense(
        num_actions,
        activation=None,
        kernel_initializer=tf.keras.initializers.RandomUniform(
            minval=-0.03, maxval=0.03),
        bias_initializer=tf.keras.initializers.Constant(-0.2))
    q_net = sequential.Sequential([flatten_layer] + dense_layers + [q_values_layer])
    optimizer = tf.keras.optimizers.Adam(learning_rate=learning_rate)
    train_step_counter = tf.Variable(0)

    agent = dqn_agent.DqnAgent(
        tf_env.time_step_spec(),
        tf_env.action_spec(),
        q_network=q_net,
        optimizer=optimizer,
        td_errors_loss_fn=common.element_wise_squared_loss,
        train_step_counter=train_step_counter)
    agent.initialize()
    return agent


# Calculate the average return of 'num_episodes'
def compute_avg_return(environment, policy, num_episodes=10):
    total_return = 0.0
    for _ in range(num_episodes):
        time_step = environment.reset()
        episode_return = 0.0

        while not time_step.is_last():
            action_step = policy.action(time_step)
            time_step = environment.step(action_step.action)
            episode_return += time_step.reward
        total_return += episode_return

    avg_return = total_return / num_episodes
    return avg_return.numpy()[0]


def collect_step(environment, policy, buffer):
    time_step = environment.current_time_step()
    action_step = policy.action(time_step)
    next_time_step = environment.step(action_step.action)
    traj = trajectory.from_transition(time_step, action_step, next_time_step)
    buffer.add_batch(traj)


def collect_data(env, policy, buffer, steps):
    for _ in range(steps):
        collect_step(env, policy, buffer)


if __name__ == '__main__':
    num_iterations = 20000
    initial_collect_steps = 100
    collect_steps_per_iteration = 3
    replay_buffer_max_length = 100000
    batch_size = 64
    learning_rate = 1e-3
    log_interval = 200
    num_eval_episodes = 10
    eval_interval = 1000

    env = suite_gym.load('containernet-v0')
    tf_env = tf_py_environment.TFPyEnvironment(env)

    fc_layer_params = (100, 50)
    action_tensor_spec = tensor_spec.from_spec(tf_env.action_spec())
    num_actions = action_tensor_spec.maximum - action_tensor_spec.minimum + 1
    agent = build_agent(fc_layer_params, num_actions)
    random_policy = random_tf_policy.RandomTFPolicy(tf_env.time_step_spec(), tf_env.action_spec())

    replay_buffer = tf_uniform_replay_buffer.TFUniformReplayBuffer(
        data_spec=agent.collect_data_spec,
        batch_size=tf_env.batch_size,
        max_length=replay_buffer_max_length)

    collect_data(tf_env, random_policy, replay_buffer, initial_collect_steps)

    dataset = replay_buffer.as_dataset(
        # num_parallel_calls=3,
        sample_batch_size=batch_size,
        num_steps=2).prefetch(3)
    iterator = iter(dataset)

    agent.train_step_counter.assign(0)

    avg_return = compute_avg_return(tf_env, agent.policy, num_eval_episodes)
    returns = [avg_return]

    print('Training...')
    for _ in range(num_iterations):
        print('Collecting data for replay buffer...')
        collect_data(tf_env, agent.collect_policy, replay_buffer, collect_steps_per_iteration)
        print('Sampling...')
        experience, unused_info = next(iterator)
        train_loss = agent.train(experience).loss
        step = agent.train_step_counter.numpy()
        if step % log_interval == 0:
            print('Step = {0}: loss = {1}'.format(step, train_loss))
        if step % eval_interval == 0:
            print('Computing current average return...')
            avg_return = compute_avg_return(tf_env, agent.policy, num_eval_episodes)
            print('Step = {0}: Average Return = {1}'.format(step, avg_return))
            returns.append(avg_return)

    print('Plotting...')
    iterations = range(0, num_iterations + 1, eval_interval)
    plt.plot(iterations, returns)
    plt.ylabel('Average Return')
    plt.xlabel('Iterations')
    plt.ylim(top=30)
    plt.savefig('avg_return.png')
