import networkx as nx
import matplotlib.pyplot as plt

from functools import reduce


def get_weight(graph, path: list) -> float:
    return reduce(lambda val, el: val + graph.get_edge_data(*el)['bw'], path, 0.0)


if __name__ == '__main__':
    edge_list = [('BS1', 'S1', {'bw': 100}), ('BS2', 'S1', {'bw': 100}), ('BS3', 'S2', {'bw': 100}),
                 ('BS4', 'S2', {'bw': 100}), ('VNF1', 'S3', {'bw': 100}), ('VNF2', 'S3', {'bw': 100}),
                 ('VNF3', 'S4', {'bw': 100}), ('VNF4', 'S4', {'bw': 100}), ('S1', 'S5', {'bw': 5}),
                 ('S1', 'S6', {'bw': 50}), ('S2', 'S5', {'bw': 10}), ('S2', 'S6', {'bw': 25}),
                 ('S3', 'S7', {'bw': 5}), ('S3', 'S8', {'bw': 20}), ('S4', 'S7', {'bw': 15}),
                 ('S4', 'S8', {'bw': 45}), ('S5', 'S9', {'bw': 80}), ('S7', 'S9', {'bw': 70}),
                 ('S6', 'S10', {'bw': 60}), ('S8', 'S10', {'bw': 70})]
    G = nx.Graph(edge_list)


    best_bw = 0.0
    best_path = []
    paths = nx.all_simple_paths(G, 'BS1', 'VNF4')
    paths_pairwise = map(nx.utils.pairwise, paths)
    for path_iter in paths_pairwise:
        path = list(path_iter)
        path_bw = reduce(lambda val, el: val + G.get_edge_data(*el)['bw'], path, 0.0)
        if path_bw > best_bw:
            best_bw = path_bw
            best_path = path
    print(best_path, best_bw)

    #nx.draw(G, with_labels=True)
    #plt.show()
