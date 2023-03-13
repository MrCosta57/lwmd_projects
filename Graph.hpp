#ifndef GRAPH_H
#define GRAPH_H

#include <vector>
#include <set>
#include <map>
#include <string>

using namespace std;

class Graph {
    private:
        long numNodes;
        long numEdges;
        string type;
        map<long, set<long>> adjList;
        
    public:
        Graph(string type);
        Graph(const vector<pair<long, long>> &edge_list, string type);
        void addEdge(long u, long v);
        void printGraph();
        map<long, set<long>> get_adjList();       
        double getDensity();
        long get_numNodes();
        long get_numEdges();
};

#endif