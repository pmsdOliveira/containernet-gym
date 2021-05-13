from containernet_backend import ContainernetBackEnd as containernet

class HostVertex:
    def __init__(self, host_vertex=None):
        if host_vertex is not None:
            self.name = host_vertex["name"]
            self.mac = host_vertex["mac"]
            self.ip = host_vertex["ip"]
            self.dimage = host_vertex["dimage"]
            self.volumes = host_vertex["volumes"]
            self.createHost()

    def createHost(self):
        containernet.create_host(self.name[-1])

class Vertex:
    def __init__(self, vertex=None):
        if vertex is not None:
            self.name = vertex["name"]
            self.type = vertex["type"]

    def getType(self):
        return self.type


class Graph:
    def __init__(self, graph=None):
        if graph is None:
            graph = {}
        self.graph = graph

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


if __name__ == '__main__':
    graph = {
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

    graph = Graph(graph=graph)
    print(graph.find_shortest_path('h1', 'h2'))
