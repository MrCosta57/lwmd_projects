/* Created by @Giovanni Costa: 880892 */
/* An implementation based on "Multicore Triangle Computations Without Tuning" by Julian Shun1, Kanat Tangwongsan*/

#include <iostream>
#include <chrono>
#include <fstream>
#include <set>
#include <string>
#include <vector>
#include <filesystem>
#include <omp.h>
#include "Graph.hpp"
#include "Set.hpp"

using namespace std;

/* GLOBAL VARIABLES */

string data_path="dataset/";
string filename="custom-dense"; //com-amazon-ungraph , as-skitter, custom-dense
string extension=".txt";
string output_path="results/";
int allow_dynamics_resources=0;


/* PROCEDURES */

/*Given an undirected graph build it's directed version based on rank by degree criteria:
  Given v in V (vertices) and u in Neigh(v), Adjacency_list[v]={u in Neigh(v) | deg(v)<=deg(u)}
  Note: The function can be executed in parallel
*/
Graph<long> rank_by_degree(Graph<long> &undirected_graph, int num_threads){
    Graph<long> result_graph("directed");
    auto adj_list=undirected_graph.get_adjList();

    //For each node v
    #pragma omp parallel num_threads(num_threads)
    for (auto it_map = adj_list.begin(); it_map!= adj_list.end(); ++it_map) {
        
        //For each u in Neigh(v)
        #pragma omp for schedule(static)
        for (auto it_set=it_map->second.begin(); it_set!=it_map->second.end(); ++it_set){
            auto neigh_id=(*it_set); //Id of vertex u
            long v_size=adj_list[it_map->first].size(); //Degree of v

            auto neigh_adj_list=adj_list.find(neigh_id); //Used "find()" to escape from '[]' operator side effect
            long u_size=0; //Degree of u, initially 0
            if (neigh_adj_list!=adj_list.end()){
                u_size=neigh_adj_list->second.size(); //If not zero degree, set it to right degree
            }

            //Add edge to direct graph respecting rank by degree criteria and breaking tie rule
            if (v_size<u_size || (v_size==u_size && it_map->first<neigh_id)){
                result_graph.addEdge(it_map->first, neigh_id); 
            }else{
                result_graph.addEdge(neigh_id, it_map->first);
            }
        }
    }
    return result_graph;
}


//Return the cardinality of the intersection between two sets
long count_intersection(Set<long>::iterator first1, Set<long>::iterator last1, 
                        Set<long>::iterator first2, Set<long>::iterator last2){
    long count=0;
    while (first1 != last1 && first2 != last2){
        if (*first1 < *first2)
            first1++;
        else if (*first2 < *first1)
            first2++;
        else{
            count++;
            first1++;
            first2++;
        }
    }
    return count;
}


/*Count how many triangle are present in an undirected graph.
NOTE: the directed graph should be builded using `rank_by_degree` function, otherwise the result may be wrong*/
long triangle_counting(Graph<long> &dir_graph, int num_threads){
    long count=0;

    auto adj_list=dir_graph.get_adjList();

    //For each node v
    #pragma omp parallel num_threads(num_threads)
    for (auto it_map = adj_list.begin(); it_map!= adj_list.end(); ++it_map) {
        
        //For each u in Neigh(v)
        #pragma omp for schedule(static) reduction (+:count)
        for (auto it_set=it_map->second.begin(); it_set!=it_map->second.end(); ++it_set){
            
            long neigh_id=(*it_set);
            auto neigh_adj_list=adj_list.find(neigh_id); //Used "find()" to escape from '[]' operator side effect

            long size=0;
            if (neigh_adj_list!=adj_list.end()){ //If one set has cardinality zero, skip intersection size computation
                size=count_intersection(it_map->second.begin(), it_map->second.end(), 
                                        neigh_adj_list->second.begin(), neigh_adj_list->second.end());
            }        
            count=count+size;
        }
    }

    return count;
}


/*Read file encoded by list of edges and store in in vector of pairs.
Node must be separated by a space and rows must be endlined correctly*/
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

    //Scan all the file
    while (inputFile >> node1 >> node2) {
        edges.push_back(make_pair(node1, node2));
    }

    // Close the file
    inputFile.close();

    return edges;
}


//Perf analysis run: ./perf stat -d -r 3 -o results/perf_dense.txt ./output.out
//Standard tun: ./output.out (after file compiling with `make` command)
int main() {
    omp_set_dynamic(allow_dynamics_resources);
    cout<<"**Graph Triangle's Counter**"<<endl;
    cout<<"System settings:"<<endl;
    cout<<"\t-No. of threads OpenMP can make: "<<omp_get_max_threads()<<endl;
    cout<<"\t-No. of core available: "<<omp_get_num_procs()<<endl;
    cout<<"\t-Dynamic scheduling allowed: "<<omp_get_dynamic()<<endl<<endl;

    stringstream ss;
    if (!(filesystem::exists(output_path+filename+extension))){ //csv header
        ss << "n_nodes,n_edges,density,num_threads,rank_time,triangle_time\n";
    }

    cout<<"Parsing "<<filename<<" ";
    auto edgle_list=parse_file(data_path+filename+extension);
    cout<<"Done!"<<endl;

    Graph<long> g(edgle_list, "undirected");
    long n_nodes=g.get_numNodes();
    long n_edges=g.get_numEdges();
    double density=g.getDensity();
    cout<<"Nodes: "<<n_nodes<<" Edges: "<<n_edges<<", "<<"Density: "<<density<<endl;

    for (int num_threads=1; num_threads<=omp_get_max_threads(); num_threads++){
        cout<<"No. THREAD: "<<num_threads<<endl;

        cout<<"\tExecuting Rank by degree function..."<<endl;
        auto start=chrono::high_resolution_clock::now();
        Graph<long> dir_g=rank_by_degree(g, num_threads);
        auto end=chrono::high_resolution_clock::now();
        auto elapsed=chrono::duration_cast<chrono::duration<double>>(end - start);
        double rank_time=elapsed.count();
        cout<<"\tDone in "<< rank_time << " secs."<<endl;
        
        cout<<"\tStarting triangles counting..."<<endl;
        start = chrono::high_resolution_clock::now();
        cout<<"\tNO. OF TRIANGLES IS: "<<triangle_counting(dir_g, num_threads)<<endl;
        end = chrono::high_resolution_clock::now();
        elapsed=chrono::duration_cast<chrono::duration<double>>(end - start);
        double triangle_time=elapsed.count();
        cout<<"\tDone in "<< triangle_time<<" secs."<<endl;
        
        ss<<to_string(n_nodes)+","+to_string(n_edges)+","+to_string(density)+","+to_string(num_threads)+","+to_string(rank_time)+","+to_string(triangle_time)+"\n";
        cout<<endl;
    }

    //Store result to .csv file in append manner
    ofstream outfile(output_path+filename+"_results.csv", ios_base::app);
    outfile << ss.str();
    outfile.close();  
    
    return 0;
}
