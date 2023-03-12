#ifndef GRAPH_H
#define GRAPH_H

#include <vector>
#include <set>

class Graph {
    private:
        int numVertices;
        int numEdges;
        string type;
        std::vector<std::set<int>> adjList;    // adjacency list representation of the graph
        
    public:
        Graph(int n);
        Graph(std::vector<std::pair<int, int>> &edge_list);
        void addEdge(int u, int v);
        void printGraph();
        int computeNumOfVertices(std::vector<std::pair<int, int>> edge_list);
        int getNumOfVertices();
        std::vector<std::set<int>> get_adjList();
        double getDensity();
};

#endif