#ifndef SET_H
#define SET_H

#include <vector>
#include <algorithm>

using namespace std;

template <typename T>
class Set{

private:
    vector<T> elements; /*Set internal representation is composed by a sorted vector, for efficiency management.
                        By set definition it's allowed only one element occurrence*/

public:
    using iterator = typename vector<T>::iterator;

    //Insert the element in the set
    bool insert(T x){
        //Use binary search to find the position to insert the element
        auto it = lower_bound(elements.begin(), elements.end(), x);

        //If the element is not already in the set, insert it
        if (it == elements.end() || *it != x){
            elements.insert(it, x);
            return true;
        }else{
            return false;
        }
    }

    //Delete the element from the set
    bool erase(T x){
        //Use binary search to find the element to erase
        auto it = std::lower_bound(elements.begin(), elements.end(), x);

        //If the element is in the set, erase it
        if (it != elements.end() && *it == x){
            elements.erase(it);
            return true;
        }else{
            return false;
        }
    }

    //Check element membership
    bool contains(T x){
        // Use binary search to find the element
        auto it =lower_bound(elements.begin(), elements.end(), x);

        // If the element is in the set, return true
        return it != elements.end() && *it == x;
    }


    size_t size(){
        return elements.size();
    }

    bool empty(){
        return elements.empty();
    }

    //Return an iterator starting at the beginning of the set
    iterator begin(){
        return elements.begin();
    }
    
    //Return an iterator that indicate the last element of the set
    iterator end(){
        return elements.end();
    }
};

#endif