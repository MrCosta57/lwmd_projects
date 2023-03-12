#VARIABLES
inputMain=main.cpp
outputMain=main.out
classes=Graph.cpp


#COMPILE
compile:
	g++ -std=c++11 $(inputMain) $(class) -o $(outputMain) -Wall -Wextra --pedantic -O3 -fsanitize=address -fsanitize=undefined

#CLEAN
clean:
	@rm -f $(outputMain)