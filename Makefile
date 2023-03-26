#VARIABLES
inputMain=main.cpp
outputMain=output.out
classes=

#COMPILE #memory leak flags: -fsanitize=address -fsanitize=undefined
compile:
	g++ -std=c++17 $(inputMain) $(classes) -o $(outputMain) -fopenmp -Wall -Wextra --pedantic -O3

#CLEAN
clean:
	@rm -f $(outputMain)