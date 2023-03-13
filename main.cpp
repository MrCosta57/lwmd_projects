/* Created by @Giovanni Costa: 880892 */
#include <iostream>
#include <chrono>
#include <fstream>
#include <cstdlib>
#include <set>
#include <string>
#include <cstring>
#include <vector>
#include <algorithm>
#include "Graph.hpp"

using namespace std;


//https://snap.stanford.edu/data/ego-Facebook.html TR_NUM=1612010 facebook_combined.txt
//https://snap.stanford.edu/data/com-Amazon.html TR_NUM=667129 com-amazon-ungraph.txt
//https://snap.stanford.edu/data/roadNet-CA.html TR_NUM=120676 roadNet-CA.txt

string prefix="dataset/";
string fileName="roadNet-CA.txt";


Graph rank_by_degree(Graph &undirected_graph){

    Graph result_graph("directed");
    auto adj_list=undirected_graph.get_adjList();

    //For each node v
    for (auto it_map = adj_list.begin(); it_map!= adj_list.end(); ++it_map) {
        //For each u contains in N(v)
        for (auto it_set=it_map->second.begin(); it_set!=it_map->second.end(); ++it_set){
            long neigh_id=(*it_set);
            long v_size=adj_list[it_map->first].size();
            long u_size=adj_list[neigh_id].size();
            if (v_size<u_size){
                result_graph.addEdge(it_map->first, neigh_id);

            }else if(v_size==u_size){
                if(it_map->first<neigh_id) result_graph.addEdge(it_map->first, neigh_id);
                else result_graph.addEdge(neigh_id, it_map->first);
            }
        }
    }
    return result_graph;
}


long triangle_counting(Graph &dir_graph){
    long count=0;

    auto adj_list=dir_graph.get_adjList();
    for (auto it_map = adj_list.begin(); it_map!= adj_list.end(); ++it_map) {
        for (auto it_set=it_map->second.begin(); it_set!=it_map->second.end(); ++it_set){
            set<long> result;
            long neigh_id=(*it_set);
            set_intersection(it_map->second.begin(), it_map->second.end(), adj_list[neigh_id].begin(), adj_list[neigh_id].end(), inserter(result, result.begin()));
            count=count+result.size();
        }
    }

    return count;
}

vector<pair<long, long>> parse_file(string path){
    // Open the file for reading
    ifstream inputFile(path);

    // Check if the file was opened successfully
    if (!inputFile.is_open()) {
        throw ifstream::failure("Failed to open file");
    }

    vector<pair<long, long>> edges;
    long node1;
    long node2;

    while (inputFile >> node1 >> node2) {
        edges.push_back(make_pair(node1, node2));
    }

    // Close the file
    inputFile.close();

    return edges;
}


int main() {
    auto start = chrono::high_resolution_clock::now();
    auto edgle_list=parse_file(prefix+fileName);
    auto end = chrono::high_resolution_clock::now();
    auto elapsed=chrono::duration_cast<chrono::duration<double>>(end - start);
    cout << "Elapsed time : "<< elapsed.count() << " secs." << endl;


    Graph g(edgle_list, "undirected");
    Graph dir_g=rank_by_degree(g);

    cout << "Starting..." << endl;
    start = chrono::high_resolution_clock::now();
    cout<<"Triangle numbers is: "<<triangle_counting(dir_g)<<endl;
    end = chrono::high_resolution_clock::now();
    elapsed=chrono::duration_cast<chrono::duration<double>>(end - start);
    cout << "Elapsed time : "<< elapsed.count() << " secs." << endl;

    cout<<"Nodes: "<<g.get_numNodes()<<" Edges: "<<g.get_numEdges()<<", "<<"Density: "<<g.getDensity()<<endl;

    



    
    return 0;
}
