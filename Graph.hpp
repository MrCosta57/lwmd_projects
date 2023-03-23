#ifndef GRAPH_H
#define GRAPH_H

#include <vector>
#include <map>
#include <string>
#include "Set.hpp"

using namespace std;

template <typename T>
class Graph {
    private:
        long numNodes;
        long numEdges;
        string type;
        map<T, Set<T>> adjList;
        
    public:
    Graph(string type="undirected") {
        this->type=type;
        numEdges=0;
        numNodes=0;
    }

    Graph(const vector<pair<T, T>> &edge_list, string type) {
        this->type=type;
        numEdges=0;
        numNodes=0;

        for (const auto &tmp_pair : edge_list){
            addEdge(tmp_pair.first, tmp_pair.second);
        }            
    }


    map<T, Set<T>> get_adjList(){
        return adjList;
    }


    void addEdge(T u, T v) {

        if (type=="directed"){
            
            bool test;
            #pragma omp critical (critical_general)
            {
            auto &map_value=adjList[u];
            test=map_value.insert(v);
            }
            if (test){
                #pragma omp atomic
                numEdges++;
            }

        }else{
            
            bool test1;
            bool test2;
            #pragma omp critical (critical_u)
            {
            auto &map_value_u=adjList[u];
            test1=map_value_u.insert(v);
            }

            #pragma omp critical (critical_v)
            {
            auto &map_value_v=adjList[v];
            test2=map_value_v.insert(u);
            }

            if (test1 || test2){
                #pragma omp atomic
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

    T get_numNodes(){
        return numNodes;
    }
    T get_numEdges(){
        return numEdges;
    }

    double getDensity(){
        double coeff=(type=="directed")? 1.0: 2.0;
        return (coeff*numEdges)/(numNodes*(numNodes-1));
    }

};

#endif