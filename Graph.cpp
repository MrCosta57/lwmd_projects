#include "Graph.hpp"
#include <iostream>
#include <algorithm>
#include <vector>
#include <string>

using namespace std;

Graph::Graph(string type="undirected") {
    this->type=type;
    numEdges=0;
    numNodes=0;
}

Graph::Graph(const vector<pair<long, long>> &edge_list, string type) {
    this->type=type;
    numEdges=0;
    numNodes=0;

    for (const auto &tmp_pair : edge_list){
        addEdge(tmp_pair.first, tmp_pair.second);
    }

    //numVertices=adjList.size();
    //numEdges=edge_list.size();
    
}


map<long, set<long>> Graph::get_adjList(){
    return adjList;
}


void Graph::addEdge(long u, long v) {

    if (type=="directed"){
        auto &map_value=adjList[u];
        bool test=map_value.insert(v).second;
        if (test){
            numEdges++;
        }
    }else{
        auto &map_value_u=adjList[u];
        auto &map_value_v=adjList[v];

        bool test1=map_value_u.insert(v).second;
        bool test2=map_value_v.insert(u).second;

        if (test1 || test2){
            numEdges++;
        }
        //cout<<"Inserted "<<u<<" and "<<v<<endl;
        //printGraph();
    }

    numNodes=adjList.size();
}


void Graph::printGraph() {
    cout<<"Graph type: "<<type<<endl;

    for (auto it_map = adjList.begin(); it_map!= adjList.end(); ++it_map) {
        cout << "Vertex "<<it_map->first<< ": { ";
        for (auto element : it_map->second) {
            cout <<element<<" ";
        }
        cout << "}" << endl;
    }
    cout<<endl;
}

long Graph::get_numNodes(){
    return numNodes;
}
long Graph::get_numEdges(){
    return numEdges;
}

double Graph::getDensity(){
    double coeff=(type=="directed")? 1.0: 2.0;
    return (coeff*numEdges)/(numNodes*(numNodes-1));
}


