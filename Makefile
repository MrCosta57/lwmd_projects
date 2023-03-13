#VARIABLES
inputMain=main.cpp
outputMain=output.out
classes=Graph.cpp


#COMPILE
compile:
	g++ -std=c++11 $(inputMain) $(classes) -o $(outputMain) -Wall -Wextra --pedantic -O3 -fsanitize=address -fsanitize=undefined

#CLEAN
clean:
	@rm -f $(outputMain)