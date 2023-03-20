/* Created by @Giovanni Costa: 880892 */
#include <iostream>
#include <chrono>
#include <fstream>
#include <set>
#include <string>
#include <algorithm>
#include <vector>
#include <omp.h>
#include "Graph.hpp"
#include "Set.hpp"

using namespace std;

//https://snap.stanford.edu/data/ego-Facebook.html TR_NUM=1612010 facebook_combined.txt
//https://snap.stanford.edu/data/com-Amazon.html TR_NUM=667129 com-amazon-ungraph.txt
//https://snap.stanford.edu/data/roadNet-CA.html TR_NUM=120676 roadNet-CA.txt

/* GLOBAL VARIABLES */
string prefix="dataset/";
string fileName="roadNet-CA.txt";
int allow_dynamics_resources=0;
bool enable_parallelism=true;


/* PROCEDURES */
Graph<int> rank_by_degree(Graph<int> &undirected_graph, bool enable_parallelism, int num_threads){
    Graph<int> result_graph("directed");
    auto adj_list=undirected_graph.get_adjList();

    //For each node v
    #pragma omp parallel if(enable_parallelism) num_threads(num_threads)
    for (auto it_map = adj_list.begin(); it_map!= adj_list.end(); ++it_map) {
        
        //For each u contains in N(v)
        #pragma omp for schedule(dynamic)
        for (auto it_set=it_map->second.begin(); it_set!=it_map->second.end(); ++it_set){
            auto neigh_id=(*it_set);
            int v_size=adj_list[it_map->first].size();
            int u_size=adj_list[neigh_id].size();

            if (v_size<u_size || (v_size==u_size && it_map->first<neigh_id)){
                result_graph.addEdge(it_map->first, neigh_id);

            }else{
                result_graph.addEdge(neigh_id, it_map->first);
            }
        }
    }
    return result_graph;
}


int count_intersection(Set<int>::iterator first1, Set<int>::iterator last1, 
                        Set<int>::iterator first2, Set<int>::iterator last2){
    int count=0;
    while (first1 != last1 && first2 != last2){
        if (*first1 < *first2)
            ++first1;
        else if (*first2 < *first1)
            ++first2;
        else{
            count++;
            first1++;
            first2++;
        }
    }
    return count;
}


int triangle_counting(Graph<int> &dir_graph, bool enable_parallelism, int num_threads){
    int count=0;

    auto adj_list=dir_graph.get_adjList();

    //For each node v
    #pragma omp parallel if(enable_parallelism) num_threads(num_threads)
    for (auto it_map = adj_list.begin(); it_map!= adj_list.end(); ++it_map) {
        
        //For each u contains in N(v)
        #pragma omp for schedule(dynamic) reduction (+:count)
        for (auto it_set=it_map->second.begin(); it_set!=it_map->second.end(); ++it_set){
            
            int neigh_id=(*it_set);
            int size=count_intersection(it_map->second.begin(), it_map->second.end(), adj_list[neigh_id].begin(), adj_list[neigh_id].end());
        
            count=count+size;
        }
    }

    return count;
}

vector<pair<int, int>> parse_file(string path){
    // Open the file for reading
    ifstream inputFile(path);

    // Check if the file was opened successfully
    if (!inputFile.is_open()) {
        throw ifstream::failure("Failed to open file");
    }

    vector<pair<int, int>> edges;
    int node1;
    int node2;

    while (inputFile >> node1 >> node2) {
        edges.push_back(make_pair(node1, node2));
    }

    // Close the file
    inputFile.close();

    return edges;
}


int main() {
    omp_set_dynamic(allow_dynamics_resources);
    int num_threads=4;

    cout<<"**Graph Triangle's Counter**"<<endl;
    cout<<"System settings:"<<endl;
    cout<<"\t-No. of threads OpenMP can make: "<<omp_get_max_threads()<<endl;
    cout<<"\t-No. of core available: "<<omp_get_num_procs()<<endl;
    cout<<"\t-Dynamic scheduling allowed: "<<omp_get_dynamic()<<endl<<endl;

    cout<<"File parsing...";
    auto edgle_list=parse_file(prefix+fileName);
    cout<<"\tDone!"<<endl;

    Graph<int> g(edgle_list, "undirected");
    cout<<"Nodes: "<<g.get_numNodes()<<" Edges: "<<g.get_numEdges()<<", "<<"Density: "<<g.getDensity()<<endl<<endl;

    cout<<"Executing Rank by degree function..."<<endl;
    auto start=chrono::high_resolution_clock::now();
    Graph<int> dir_g=rank_by_degree(g, enable_parallelism, num_threads);
    auto end=chrono::high_resolution_clock::now();
    auto elapsed=chrono::duration_cast<chrono::duration<double>>(end - start);
    cout<<"Done in "<< elapsed.count() << " secs."<<endl<<endl;
    
    cout<<"Starting triangles counting..."<<endl;
    start = chrono::high_resolution_clock::now();
    cout<<"NO. OF TRIANGLES IS: "<<triangle_counting(dir_g, enable_parallelism, num_threads)<<endl;
    end = chrono::high_resolution_clock::now();
    elapsed=chrono::duration_cast<chrono::duration<double>>(end - start);
    cout<<"Done in "<< elapsed.count()<<" secs."<<endl;

    
    return 0;
}
