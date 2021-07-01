from containernet_backend import ContainernetBackEnd as Containernet


DOCKER_VOLUMES = ["/home/pmsdoliveira/workspace/containers/vol1/:/home/vol1"]
N_HOSTS = 8
N_SWITCHES = 10
HOST_SWITCH_LINKS = [0, 9, 8, 7, 2, 1, 6, 5]


class HostVertex:
    def __init__(self, backend: Containernet, params: dict) -> None:
        self.host = backend.create_host(params['name'], params['mac'], params['ip'],
                                        'iperf:latest', DOCKER_VOLUMES)


class SwitchVertex:
    def __init__(self, backend: Containernet, params: dict) -> None:
        self.switch = backend.create_switch(params['name'], 'OpenFlow13')


class Edge:
    def __init__(self, backend: Containernet, origin: str, destination: str) -> None:
        self.origin = origin
        self.destination = destination
        backend.create_host_switch_links(HOST_SWITCH_LINKS)

class Graph:
    def __init__(self, backend: Containernet, structure=None):
        self.backend = backend
        if structure is None:
            self.structure = {}
        else:
            self.structure = structure

        self.host_vertexes = []

    def add_vertex(self, vertex):
        if vertex not in self.structure:
            self.structure[vertex] = []

    """
    def getVertices(self):
        return list(self.graph.keys())

    def getEdges(self):
        edges = []
        for vertex in self.graph:
            for next_vertex in self.graph:
                if {next_vertex, vertex} not in edges:
                    edges.append({vertex, next_vertex})
        return edges

    def addVertex(self, vertex):
        if vertex not in self.graph:
            self.graph[vertex] = []

    def addEdge(self, edge):
        edge = set(edge)
        (vertex1, vertex2) = tuple(edge)
        if vertex1 in self.graph:
            self.graph[vertex1].append[vertex2]
        else:
            self.graph[vertex1] = [vertex2]

    def findPath(self, start, end, path=[]):
        if start not in self.graph or end not in self.graph:
            return None
        path = path + [start]
        if start == end:
            return path

        for node in self.graph[start]:
            if node not in path:
                newpath = self.findPath(node, end, path)
                if newpath:
                    return newpath
        return None

    def find_all_paths(self, start, end, path=[]):
        if start not in self.graph or end not in self.graph:
            return []
        path = path + [start]
        if start == end:
            return [path]

        paths = []
        for node in self.graph[start]:
            if node not in path:
                newpaths = self.find_all_paths(node, end, path)
                for newpath in newpaths:
                    paths.append(newpath)
        return paths

    def find_shortest_path(self, start, end, path=[]):
        if start not in self.graph or end not in self.graph:
            return None
        path = path + [start]
        if start == end:
            return path

        shortest = None
        for node in self.graph[start]:
            if node not in path:
                newpath = self.find_shortest_path(node, end, path)
                if newpath:
                    if not shortest or len(newpath) < len(shortest):
                        shortest = newpath
        return shortest
    """


if __name__ == '__main__':
    graph_description = {
        'h1': ['s1'],
        'h2': ['s10'],
        'h3': ['s9'],
        'h4': ['s8'],
        'h5': ['s3'],
        'h6': ['s2'],
        'h7': ['s7'],
        'h8': ['s6'],
        's1': ['h1', 's2', 's3'],
        's2': ['h6', 's1', 's3', 's4', 's6'],
        's3': ['h5', 's1', 's2', 's4', 's8'],
        's4': ['s2', 's3', 's5', 's6', 's7'],
        's5': ['s4', 's7', 's8', 's9', 's10'],
        's6': ['h8', 's2', 's4', 's7'],
        's7': ['h7', 's4', 's5', 's6', 's10'],
        's8': ['h4', 's3', 's5', 's9'],
        's9': ['h3', 's5', 's8'],
        's10': ['h2', 's5', 's7']
    }

    hosts_descriptions = [
        {
            'name': 'h1',
            'mac': '00:00:00:00:00:01',
            'ip': '10.0.0.1',
        },
        {
            'name': 'h2',
            'mac': '00:00:00:00:00:02',
            'ip': '10.0.0.2',
        },
        {
            'name': 'h3',
            'mac': '00:00:00:00:00:03',
            'ip': '10.0.0.3',
        },
        {
            'name': 'h4',
            'mac': '00:00:00:00:00:04',
            'ip': '10.0.0.4',
        },
    ]


    switches_descriptions = [
        {'name': 's1'}, {'name': 's2'}, {'name': 's3'}, {'name': 's4'}, {'name': 's5'}, {'name': 's6'}, {'name': 's7'},
        {'name': 's8'}, {'name': 's9'}, {'name': 's10'}
    ]
    be = Containernet()
    hosts = [HostVertex(be, params=hosts_descriptions[host_idx]) for host_idx in range(N_HOSTS)]
    switches = [SwitchVertex(be, params=switches_descriptions[switch_idx]) for switch_idx in range(N_SWITCHES)]
    be.net.start()
    print("Done")
    be.net.stop()
