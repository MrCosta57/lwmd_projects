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
#include "Graph.h"

using namespace std;

//http://snap.stanford.edu/data/email-Eu-core.html TR_NUM=105461

string prefix="dataset/";
string fileName="email-Eu-core.txt";


Graph rank_by_degree(Graph undirected_graph){
    Graph result_graph(undirected_graph.getNumOfVertices());

    auto adj_list=undirected_graph.get_adjList();

    for (int i=0; i<adj_list.size(); i++){
        int current_deg=adj_list[i].size();

        auto condition = [current_deg, adj_list](int id) { return current_deg, adj_list[id].size(); };
        //mySet.erase(std::remove_if(mySet.begin(), mySet.end(), condition), mySet.end());
    }

}


int main() {

    // Open the file for reading
    ifstream inputFile(prefix+fileName);

    // Check if the file was opened successfully
    if (!inputFile.is_open()) {
        cerr << "Failed to open file" <<endl;
        return 1;
    }

    // Read the file line by line
    string line;
    vector<pair<int, int>> edges;
    while (std::getline(inputFile, line)) {
        int v1= stoi(line.substr(0, line.find(" ")));
        int v2= stoi(line.substr(line.find(" "), line.size()));
        edges.push_back((v1<=v2) ? make_pair(v1, v2) : make_pair(v2, v1));
    }

    // Close the file
    inputFile.close();

    sort(edges.begin(), edges.end());


    edges.erase( unique( edges.begin(), edges.end() ), edges.end() );

    








    // create a graph with 5 vertices
    Graph g(5);
    
    // add some edges to the graph
    g.addEdge(0, 1);
    g.addEdge(0, 4);
    g.addEdge(1, 2);
    g.addEdge(1, 3);
    g.addEdge(1, 4);
    g.addEdge(2, 3);
    g.addEdge(3, 4);
    
    // print the graph
    g.printGraph();
    
    return 0;
}
