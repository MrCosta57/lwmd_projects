#ifndef GRAPH_H
#define GRAPH_H

#include <vector>
#include <map>
#include <string>
#include "Set.hpp"

using namespace std;

class Graph {
    private:
        long numNodes;
        long numEdges;
        string type;
        map<long, Set<long>> adjList;
        
    public:
    Graph(string type="undirected") {
        this->type=type;
        numEdges=0;
        numNodes=0;
    }

    Graph(const vector<pair<long, long>> &edge_list, string type) {
        this->type=type;
        numEdges=0;
        numNodes=0;

        for (const auto &tmp_pair : edge_list){
            addEdge(tmp_pair.first, tmp_pair.second);
        }            
    }


    map<long, Set<long>> get_adjList(){
        return adjList;
    }


    void addEdge(long u, long v) {

        if (type=="directed"){
            auto &map_value=adjList[u];
            bool test=map_value.insert(v);
            if (test){
                numEdges++;
            }
        }else{
            auto &map_value_u=adjList[u];
            auto &map_value_v=adjList[v];

            bool test1=map_value_u.insert(v);
            bool test2=map_value_v.insert(u);

            if (test1 || test2){
                numEdges++;
            }
            //cout<<"Inserted "<<u<<" and "<<v<<endl;
            //printGraph();
        }

        numNodes=adjList.size();
    }


    void printGraph() {
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

    long get_numNodes(){
        return numNodes;
    }
    long get_numEdges(){
        return numEdges;
    }

    double getDensity(){
        double coeff=(type=="directed")? 1.0: 2.0;
        return (coeff*numEdges)/(numNodes*(numNodes-1));
    }

};

#endif