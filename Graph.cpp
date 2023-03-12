#include "Graph.h"
#include <iostream>
#include <algorithm>
#include <vector>

using namespace std;


Graph::Graph(int n) {
    numVertices = n;
    adjList.resize(n);
}

Graph::Graph(vector<pair<int, int>> &edge_list) {
    
    numEdges=edge_list.size();
    numVertices=computeNumOfVertices(edge_list);
    adjList.resize(this->numVertices);

    for (const auto &tmp_pair : edge_list){
        addEdge(tmp_pair.first, tmp_pair.second);
    }
}

int Graph::computeNumOfVertices(vector<pair<int, int>> edge_list){
    sort(edge_list.begin(), edge_list.end());

    int count=0;
    for (int i=0; i<edge_list.size()-1; i++){
        if (edge_list[i].first!=edge_list[i+1].first){
            count++;
        }
    }

    return count;
}

int Graph::getNumOfVertices(){
    return numVertices;
}

std::vector<std::set<int>> Graph::get_adjList(){
    return adjList;
}


void Graph::addEdge(int u, int v) {
    adjList[u].insert(v);
    adjList[v].insert(u);
}

void Graph::printGraph() {

    for (int i = 0; i < adjList.size(); i++) {
        cout << "Vertex "<< i<< ": ";
        for (const auto& element : adjList[i]) {
            cout << element << "{";
        }
        cout << "}" << endl;
  }
}
