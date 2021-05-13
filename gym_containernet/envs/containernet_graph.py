class Graph:
    def __init__(self, graph=None):
        if graph is None:
            graph = []
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
        else:
            self.graph[vertex1] = [vertex2]
